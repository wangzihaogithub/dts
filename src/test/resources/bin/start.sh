#! /bin/sh

# 当前服务地址
cd `dirname $0`
service_path=$(pwd)
echo ${service_path}


# Nohup文件地址
nohup_file="${service_path}/nohup.log"
echo "nohup_file:" ${nohup_file}

# 日志路径
log_path=$(mkdir -p /data/logs/cnwy-dts; cd /data/logs/cnwy-dts; pwd)
echo "log_path:" ${log_path}

# JvmConfig
#jvm_opts="-Xms4g -Xmx4g -Xmn2g -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+UseCMSCompactAtFullCollection -XX:CMSFullGCsBeforeCompaction=0 -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:${log_path}/gc.log"
jvm_opts="-Xms2g -Xmx2g -Xmn1g -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+UseCMSCompactAtFullCollection -XX:CMSFullGCsBeforeCompaction=0 -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:${log_path}/gc.log"
echo "jvm_opts:" ${jvm_opts}

# 可执行Jar包地址
jar_file=${service_path}/../cnwy-dts.jar
echo "jar_file:" ${jar_file}

#remote_debug="-agentlib:jdwp=transport=dt_socket,com.tbk.com.tbk.server=y,suspend=n,address=8996"


#nohup java ${remote_debug}  ${jvm_opts} -jar   ${jar_file}  >${nohup_file} 2>&1 &
nohup java -Djava.security.egd=file:/dev/./urandom -Dfile.encoding=utf-8 ${jvm_opts} -jar   ${jar_file} --cnwy.profile=/files >${nohup_file} 2>&1 &

