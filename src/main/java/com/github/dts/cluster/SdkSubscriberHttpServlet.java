package com.github.dts.cluster;

import com.github.dts.conf.ConfigSdkLoginService;
import com.github.dts.util.CanalConfig;

import javax.servlet.*;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

public class SdkSubscriberHttpServlet extends HttpServlet {
    private final SdkSubscriber sdkSubscriber;
    private final DiscoveryService discoveryService;
    private final ConfigSdkLoginService configSdkLoginService;

    public SdkSubscriberHttpServlet(SdkSubscriber sdkSubscriber, CanalConfig canalConfig, DiscoveryService discoveryService) {
        this.sdkSubscriber = sdkSubscriber;
        this.discoveryService = discoveryService;
        this.configSdkLoginService = new ConfigSdkLoginService(canalConfig);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String authorization = req.getHeader("Authorization");
        if (authorization == null || authorization.isEmpty()) {
            authorization = req.getParameter("Authorization");
        }
        String fetch = req.getHeader("Authorization-fetch");

        Principal principal = "true".equalsIgnoreCase(fetch) ? fetchSdk(authorization) : loginSdk(authorization);
        if (principal == null) {
            resp.setHeader("WWW-Authenticate", "Basic realm=\"SdkSubscriberHttpServlet\"");
            resp.sendError(HttpServletResponse.SC_UNAUTHORIZED);
        } else {
            AsyncContext asyncContext = req.startAsync();
            asyncContext.setTimeout(-1);
            HttpServletResponse response = (HttpServletResponse) asyncContext.getResponse();
            response.setContentType("text/event-stream");
            response.setCharacterEncoding("UTF-8");
            response.setStatus(200);
            response.flushBuffer();
            HttpSdkChannel channel = new HttpSdkChannel(principal, asyncContext);
            asyncContext.addListener(new CloseAsyncListener(channel, sdkSubscriber));
            sdkSubscriber.add(channel);
        }
    }

    protected Principal loginSdk(String authorization) {
        Principal principal = configSdkLoginService.loginSdk(authorization);
        if (principal == null && discoveryService != null) {
            principal = discoveryService.loginSdk(authorization);
        }
        return principal;
    }

    protected Principal fetchSdk(String authorization) {
        Principal principal = configSdkLoginService.fetchSdk(authorization);
        if (principal == null && discoveryService != null) {
            principal = discoveryService.fetchSdk(authorization);
        }
        return principal;
    }

    private static class HttpSdkChannel implements SdkChannel {
        private final Principal principal;
        private final AsyncContext asyncContext;
        private final ServletRequest request;
        private final ServletResponse response;
        private final LinkedBlockingQueue<SdkMessage> writeMessageList = new LinkedBlockingQueue<>();
        private volatile boolean close;

        private HttpSdkChannel(Principal principal, AsyncContext asyncContext) {
            this.principal = principal;
            this.asyncContext = asyncContext;
            this.request = asyncContext.getRequest();
            this.response = asyncContext.getResponse();
        }

        @Override
        public Principal getPrincipal() {
            return principal;
        }

        @Override
        public boolean isOpen() {
            return !close;
        }

        @Override
        public void write(SdkMessage writeMessage) {
            writeMessageList.add(writeMessage);
        }

        @Override
        public void flush() throws IOException {
            int size = writeMessageList.size();
            if (size == 0) {
                return;
            }

            ArrayList<SdkMessage> list = new ArrayList<>(size);
            writeMessageList.drainTo(list);

            ServletOutputStream outputStream = response.getOutputStream();
            synchronized (this) {
                for (SdkMessage sdkMessage : list) {
                    outputStream.write(sdkMessage.toSseBytes());
                }
            }
            outputStream.flush();
        }

        @Override
        public void close() {
            this.close = true;
        }
    }

    private static class CloseAsyncListener implements AsyncListener {
        private final HttpSdkChannel channel;
        private final SdkSubscriber sdkSubscriber;

        CloseAsyncListener(HttpSdkChannel channel, SdkSubscriber sdkSubscriber) {
            this.channel = channel;
            this.sdkSubscriber = sdkSubscriber;
        }

        @Override
        public void onComplete(AsyncEvent event) throws IOException {
            sdkSubscriber.remove(channel);
        }

        @Override
        public void onTimeout(AsyncEvent event) throws IOException {
            sdkSubscriber.remove(channel);
        }

        @Override
        public void onError(AsyncEvent event) throws IOException {
            sdkSubscriber.remove(channel);
        }

        @Override
        public void onStartAsync(AsyncEvent event) throws IOException {

        }
    }


}