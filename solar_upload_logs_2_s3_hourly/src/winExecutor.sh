source /etc/profile
source ~/.bash_profile
export LANG="zh_CN.utf8"

#DOMAIN="envision"
USER_NAME="oceanlog"
PASS_WORD="oceanlog_123"
IPS="10.22.13.26 10.22.13.151"
#IPS="10.22.13.143"

for IP in ${IPS} 
do
	delaySec=$[$RANDOM%3]
	echo -e "\aprocess will execute on $ip after $delaySec seconds"
	sleep $delaySec
	if [  -z ${DOMAIN} ] || [ ${DOMAIN}  == "" ] ;then
	
		RUN_SHELL="winexe -U ${USER_NAME}%${PASS_WORD} //${IP} 'cmd.exe /C "
	else
		RUN_SHELL="winexe -U ${DOMAIN}/${USER_NAME}%${PASS_WORD} //${IP} 'cmd.exe /C "
	fi	

	cmd="$RUN_SHELL $@'"
#	echo $cmd
	eval $cmd
    CODE=$?
    echo $CODE    
    if [ $CODE -ne 1 ] ;then
        exit $CODE
    fi 

done

