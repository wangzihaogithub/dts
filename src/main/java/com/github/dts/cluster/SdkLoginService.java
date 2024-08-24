package com.github.dts.cluster;

import java.security.Principal;

public interface SdkLoginService {
    Principal loginSdk(String authorization);

    Principal fetchSdk(String authorization);
}