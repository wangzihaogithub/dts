#! /bin/sh

pid=`ps -ef | grep cnwy-dts.jar | grep java | awk -F" " '{print $2}'`
echo "stop pid:" ${pid}

# 不强制停止
if [[ -n "${pid}" ]]; then
    kill ${pid}
else
    echo "no such process!"
fi
time=0
while [ $time -ge 0 ]; do
    sleep 1
    pid=`ps -ef | grep cnwy-dts.jar | grep java | awk -F" " '{print $2}'`
    if [[ -n "${pid}" ]]; then
        if [ $time -ge 30 ];then
            kill -9 $pid;
            break
        fi
    else
      break
    fi
    time=`expr $time + 1 `
done
echo "stop finish"
