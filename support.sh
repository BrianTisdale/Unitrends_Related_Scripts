#!/bin/sh
#
# Author:  Brian Tisdale
# Last Update:  04/03/2019
#
# 1. The purpose of this script is to increase Unitrends Customer Engineer efficiency.
# 2. As the user of this script, if you do not know how to accomplish your task without the script, then you are not qualified to have this script do the task for you.
#

DEBUG="NO"

########################################
###  Do not edit below this line!!!  ###
########################################

VERSION="9.0.1"

TYPECASE=" case b.type when 1 then 'Mast' when 2 then 'Diff' when 3 then 'Incr' when 4 then 'Sele' when 5 then 'ExFu' when 6 then 'ExDi' when 7 then 'ExIn' when 9 then 'SQLF' when 10 then 'SQLD' when 11 then 'SQLT' when 12 then 'VMFu' when 13 then 'VMDi' when 14 then 'VMIn' when 16 then 'Rest' when 80 then 'IBMR' when 96 then 'WIRR' when 128 then 'Veri' when 256 then 'Veri' when 512 then 'Veri' when 1024 then 'BM' when 1031 then 'HVFu' when 1032 then 'HVDi' when 1033 then 'HVIn' when 1036 then 'SPFu' when 1037 then 'SPDi' when 1038 then 'Meta' when 1039 then 'UCSF' when 1034 then 'OraF' when 1035 then 'OraI' when 4098 then 'NDFu' when 4099 then 'NDDi' when 4100 then 'NDIn' when 4101 then 'XeFu' when 112 then 'Repl' when 4103 then 'AHVF' when 4105 then 'AHVI' when 4106 then 'IFul' when 4107 then 'IDif' when 4108 then 'IInc' when 2080 then 'IRes' else 'Unkn' end "
ATYPECASE=" case a.backtype when 1 then 'Mast' when 2 then 'Diff' when 3 then 'Incr' when 4 then 'Sele' when 5 then 'ExFu' when 6 then 'ExDi' when 7 then 'ExIn' when 9 then 'SQLF' when 10 then 'SQLD' when 11 then 'SQLT' when 12 then 'VMFu' when 13 then 'VMDi' when 14 then 'VMIn' when 16 then 'Rest' when 80 then 'IBMR' when 96 then 'WIRR' when 128 then 'Veri' when 256 then 'Veri' when 512 then 'Veri' when 1024 then 'BM' when 1031 then 'HVFu' when 1032 then 'HVDi' when 1033 then 'HVIn' when 1036 then 'SPFu' when 1037 then 'SPDi' when 1038 then 'Meta' when 1039 then 'UCSF' when 1034 then 'OraF' when 4098 then 'NDFu' when 4099 then 'NDDi' when 4100 then 'NDIn' else 'Unkn' end "
STYPECASE=" case s.type when 1 then 'Mast' when 2 then 'Diff' when 17 then 'Incr' when 10 then 'SQLF' when 11 then 'SQLD' when 13 then 'Arch' when 16 then 'VMIn' when 37 then 'IInc' when 28 then 'NDFu' when 30 then 'NDIn' else 'Unkn' end "
SSTATUSCASE=" case (select status from bp.replication_queue where backup_no = b.backup_no order by position desc limit 1) when 1 then 'pending' when 2 then 'done' when 4 then 'failed' when 8 then 'aborted' when 16 then 'active' when 32 then 'terminated' when 64 then 'escalate' when 128 then 'paused' when 256 then 'purged' when 512 then 'skipped' when 1024 then 'fixed' when 0 then 'n/a' else '???' end "
PURGED="4294967295"

RED="\e[1;31m"
GREEN="\e[;32m"
YELLOW="\e[;33m"
BLUE="\e[;34m"
MAGENTA="\e[;35m"
CYAN="\e[1;36m"
NOCOLOR="\e[0m"

debug(){
	if [[ "${DEBUG}" == "YES" ]]; then
		echo -e "${CYAN}Debug:  ${*}${NOCOLOR}"
	fi
}

rosVersion(){
	debug "rosVersion"
	rpm -qa unitrends-release | awk -F '-' ' { print $3 } '
}

dca_history_chart(){
	debug "dca_history_chart"
	shift
	WHERE=""
	if [[ "${1}" == "-j" ]]; then
		WHERE=" where j.id = ${2} "
	fi
	psql rdrdb -c "select j.id as jid, s.id as sid, j.name, to_timestamp(cast (start_time as TEXT), 'YYYY-MM-DD HH24:MI:SS') as start_time, to_timestamp(cast (end_time as TEXT), 'YYYY-MM-DD HH24:MI:SS') as end_time, case result when 1 then 'successful' when 2 then 'failed' when 4 then 'warning' when 6 then 'cancelled' when 8 then 'already_running' else 'unknown' end as status from job j join job_session s on j.id = s.job_id ${WHERE} order by s.id desc"
}

error(){
	echo -e "${RED}Error:  ${*}${NOCOLOR}"
	exit 1
}

applianceInfoHeader(){
	debug "applianceInfoHeader"
	NAME="$(hostname)"
	ASSET="$(/usr/local/bin/dpu asset | tail -n 1 | awk ' { print $NF } ')"
	IP="$(hostname -i)"
	DATE="$(date)"
	echo -e "${NAME}\n${ASSET}\n${IP}\n${DATE}"
}

backup_statistics(){
	debug "backup_statistics"
	NUMDAYS=${2:-7}
	TODAY=$(date -d $(date +%m/%d/%Y) +%s)
	
	applianceInfoHeader
	echo
	echo -e "Local backups' statuses for the last ${NUMDAYS} days:"
	echo -e "-------------------------------------------------------------------\c"
	
	COUNT=$(echo ${NUMDAYS}-1 | bc)
	
	while [[ "${COUNT}" -ge "0" ]]
	do
		echo
	        STARTDATE=$(echo "${TODAY}-(${COUNT}*86400)" | bc)
	        echo -e "$(date -d @${STARTDATE} +%m/%d/%Y):  \c"
	        ENDDATE=$(echo ${STARTDATE}+86400 | bc)
		QUERY="select count(*) from bp.backups b join bp.nodes n using(node_no) where b.start_time >= ${STARTDATE} and b.start_time < ${ENDDATE} and b.status in (512, 514, 524800) and b.tape_no <> ${PURGED} and n.system_id is NULL"
		debug "QUERY=${QUERY}"
	        ACTIVE="$(psql bpdb -U postgres -A -t -c "${QUERY}")"

		QUERY="select count(*) from bp.jobs j where j.queued_date >${STARTDATE} and j.queued_date < ${ENDDATE} and j.start_time = 0"
		debug "QUERY=${QUERY}"
		QUEUED="$(psql bpdb -U postgres -A -t -c "${QUERY}")"

		QUERY="select count(*) from bp.successful_backups b join bp.nodes n using(node_no) where b.start_time >= ${STARTDATE} and b.start_time < ${ENDDATE} and b.tape_no <> ${PURGED} and n.system_id is NULL"
		debug "QUERY=${QUERY}"
	        SUCCESSFUL="$(psql bpdb -U postgres -A -t -c "${QUERY}")"

		QUERY="select count(*) from bp.backups b join bp.nodes n using(node_no) where b.start_time >= ${STARTDATE} and b.start_time < ${ENDDATE} and b.status not in (512, 514, 524800) and b.tape_no <> ${PURGED} and b.backup_no not in (select backup_no from bp.successful_backups) and n.system_id is NULL"
		debug "QUERY=${QUERY}"
	        FAILED="$(psql bpdb -U postgres -A -t -c "${QUERY}")"

	        echo -e "${ACTIVE} active, ${QUEUED} queued, ${SUCCESSFUL} successful, ${FAILED} failed\c"
	        ((COUNT=${COUNT}-1))
	done
	echo " (as of $(date +%H:%m:%S))"
}

backup_history_chart(){
        debug "backup_history_chart"
        CLIENTLIST=""
	GCLIENTLIST=""
	INSTANCEIDLIST=""
	SYSTEMIDLIST=""
	TYPELIST=""
	INSTANCELIST=""
	JOBSLIST=""
	STATS=""
	BEND=""
	BACKUPNOLIST=""
	SOURCELIST=""
	SORTCOLS=" order by backup_no "
	SORTORDER=" desc "
	LIMIT=" limit 999999 "
	SHOWCLIENT="(select coalesce(nullif((select n.gcname || ' (' || n.node_name || ')'), (select ' (' || n.node_name || ')')), n.node_name)) as client"
	SHOWEXTENDED=""
	NOMETA=" and b.type <> 1038 "
	NOPURGE=""
	NOSYNTH=""
	LAST24=""
	IMPORTED=""
	NOACTIVE=""
	TABLE=" bp.backups b "
	COLS1=""
	COLS2=""
	EXTRA=""
	NEWER=""
	CURRENTINSTANCES=""
	SHOWLOCALBACKUPS=""
	SHOWRET=""
	JOINRET=""
	TOCSV="NO"
	TODOHEADING=""
	for NUM in `seq 2 ${#}`
	do
		CURRENT=`echo ${*} | cut -f ${NUM} -d ' '`
		case ${CURRENT} in 
			-c)
				if [[ "${GCLIENTLIST}" != "" ]]; then
					error "Cannot use both -c and --gc."
				fi
				CLIENTLIST=$(echo ${*} | cut -f `echo ${NUM}+1 | bc` -d ' ')
				CLIENTLIST=`echo ${CLIENTLIST} | sed "s/^/'/" | sed "s/$/'/" | sed "s/,/','/g"`
				CLIENTLIST=" and n.node_name in (${CLIENTLIST}) "
				NUM=`echo ${NUM}+1 | bc`
				;;
			--gc)
				if [[ "${CLIENTLIST}" != "" ]]; then
					error "Cannot use both -c and --gc."
				fi
				GCLIENTLIST=$(echo ${*} | cut -f `echo ${NUM}+1 | bc` -d ' ')
				GCLIENTLIST=`echo ${GCLIENTLIST} | sed "s/^/'/" | sed "s/$/'/" | sed "s/,/','/g"`
				if [[ "${GCLIENTLIST}" == "'99'" ]]; then
					GCLIENTLIST=""
				else
					GCLIENTLIST=" and n.gcname in (${GCLIENTLIST}) "
				fi
				SHOWCLIENT="n.gcname as client"
				NUM=`echo ${NUM}+1 | bc`
				;;
			-j)
				JOBSLIST=$(echo ${*} | cut -f `echo ${NUM}+1 | bc` -d ' ')
				JOBSLIST=" AND b.job_no in (${JOBSLIST}) "
				NUM=`echo ${NUM}+1 | bc`
				echo
				;;
			-t)
				TYPELIST=$(echo ${*} | cut -f `echo ${NUM}+1 | bc` -d ' ')
				TYPELIST=" AND b.type in (${TYPELIST}) "
				NUM=`echo ${NUM}+1 | bc`
				echo
				;;
			--current)
				CURRENTINSTANCES=" and i.current = true "
				;;
			--retention)
				SHOWRET=" , case when (select backup_no from bp.successful_backups where tape_no <> ${PURGED} and instance_id = i.instance_id and type in (1,5,9,12,1031,1024,4106,1038) order by backup_no desc limit 1) <= backup_no then 'last' else 'n/a' end as last_backup, (($(date +%s)-b.start_time)*1.0/86400)::numeric(10,2) as age_days, l.min_limit as min, l.max_limit as max, l.legal_hold as legal, b.properties, (b.properties & 131072) != 0 as p_daily, (b.properties & 262144) != 0 as p_weekly, (b.properties & 524288) != 0 as p_monthly, (b.properties & 1048576) != 0 as p_yearly, g.days as s_days, g.weekly as s_weekly, g.monthly as s_monthly, g.yearly as s_yearly, h.compliant "
				JOINRET=" left join bp.retention_limits l on i.instance_id = l.instance_id left join bp.gfs_policy_association h on i.instance_id =h.instance_id left join bp.gfs_policy g using(policy_id) "
				;;
			-s)
				SOURCELIST=$(echo ${*} | cut -f `echo ${NUM}+1 | bc` -d ' ')
                                SOURCELIST=`echo ${SOURCELIST} | sed "s/^/'/" | sed "s/$/'/" | sed "s/,/','/g"`
                                SOURCELIST=" and n.gcname in (${SOURCELIST}) "
				NUM=`echo ${NUM}+1 | bc`
				;;
			-i)
				COUNT=1
				while true; do
					NEWITEM="$(echo ${*} | cut -f `echo ${NUM}+${COUNT} | bc` -d ' ')"
					if [[ "${NEWITEM}" == "" ]]; then
						break
					fi
					echo "${NEWITEM}" | grep -q -- "^-"
					if [[ $? != 0 ]]; then
						INSTANCELIST="${INSTANCELIST} ${NEWITEM}"
						((COUNT=${COUNT}+1))
					else
						break
					fi
				done
				((COUNT=${COUNT}-1))
				((NUM=${NUM}+${COUNT}))
				INSTANCELIST=`echo ${INSTANCELIST} | sed "s/^/'/" | sed "s/$/'/" | sed "s/,/','/g"`
				INSTANCELIST=" and (i.key1 in (${INSTANCELIST}) or i.key2 in (${INSTANCELIST}) or q.server_instance_name in (${INSTANCELIST}) or q.database_name in (${INSTANCELIST})) "
				NUM=`echo ${NUM}+1 | bc`
				echo
				;;
			-b)
				BACKUPNOLIST=$(echo ${*} | cut -f `echo ${NUM}+1 | bc` -d ' ')
				BACKUPNOLIST=" and b.backup_no in (${BACKUPNOLIST}) "
				NUM=`echo ${NUM}+1 | bc`
				echo
				;;
			--newer)
				NEWER=" and b.backup_no > $(echo ${*} | cut -f `echo ${NUM}+1 | bc` -d ' ') "
				NUM=`echo ${NUM}+1 | bc`
				echo
				;;
			--todo)
				QUERY="select b.backup_no from bp.nodes n join bp.application_instances i using (node_no) join bp.backups b using (instance_id) where backup_no in (select backup_no from bp.backups where backup_no not in (select backup_no from bp.successful_backups) and status not in (512, 514, 524800)) and backup_no in (select backup_no from bp.backups where instance_id = i.instance_id order by backup_no desc limit 1) order by backup_no desc"
				debug "QUERY=${QUERY}"
				BACKUPNOLIST=$(echo $(psql bpdb -U postgres -A -t -c "${QUERY}") | sed "s/ /,/g")
				if [[ "${BACKUPNOLIST}" == "" ]]; then
					echo
					echo "$(hostname) - There are no instances who have a latest backup that is failed."
					exit 0
				fi
				BACKUPNOLIST=" and b.backup_no in (${BACKUPNOLIST}) "
				TODOHEADING="$(hostname) - Items having latest backup attempts that failed:\n-------------------------------------------------------------------"
				NUM=`echo ${NUM}+1 | bc`
				echo
				;;
			--sid)
				SYSTEMIDLIST=$(echo ${*} | cut -f `echo ${NUM}+1 | bc` -d ' ')
				if [[ "${SYSTEMIDLIST}" == "99" ]]; then
					SYSTEMIDLIST=" "
				else
					SYSTEMIDLIST=" and s.system_id in (${SYSTEMIDLIST}) "
					NUM=`echo ${NUM}+1 | bc`
				fi
				echo
				;;
			--iid)
				INSTANCEIDLIST=$(echo ${*} | cut -f `echo ${NUM}+1 | bc` -d ' ')
				INSTANCEIDLIST=" and i.instance_id in (${INSTANCEIDLIST}) "
				NUM=`echo ${NUM}+1 | bc`
				echo
				;;
			--meta)
				NOMETA=" "
				;;
			--nopurge)
				NOPURGE=" and b.tape_no <> ${PURGED} "
				;;
			--success)
				TABLE=" bp.successful_backups b "
				;;
			--active)
				TABLE=" (select * from bp.backups where status in (514, 512, 524800)) b "
				;;
			--failed)
				TABLE=" (select * from bp.backups where backup_no not in (select backup_no from bp.successful_backups) and status not in (3168, 3072, 514, 524800, 512)) b "
				;;
			-e)
				SHOWEXTENDED=" , (b.properties & 1024) <> 0 as dedup, (b.properties & 512) <> 0 as hash, (b.properties & 4) <> 0 as comp, (b.properties & 256) <> 0 as enc "
				;;
			--synth)
				NOSYNTH=" and ((b.properties & 2048) != 0) = true "
				;;
			--nosynth)
				NOSYNTH=" and ((b.properties & 2048) != 0) = false "
				;;
			--sort)
				SORTCOLS=" order by $(echo ${*} | cut -f `echo ${NUM}+1 | bc` -d ' ') "
				SORTORDER=" $(echo ${*} | cut -f `echo ${NUM}+2 | bc` -d ' ') "
				;;
			--limit)
				LIMIT=" limit $(echo ${*} | cut -f `echo ${NUM}+1 | bc` -d ' ') "
				NUM=`echo ${NUM}+1 | bc`
				;;
			--stats)
				STATS=" , case b.total_megs when 0 then 0 else case b.elapsed_time when 0 then cast((b.total_megs*1.0/(b.elapsed_time+1)) as numeric(15,2)) else cast((b.total_megs*1.0/b.elapsed_time) as numeric(15,2)) end end as MBps, case b.total_megs when 0 then 0 else case b.elapsed_time when 0 then cast((b.total_megs*1.0/(b.elapsed_time+1))*60 as numeric(15,2)) else cast((b.total_megs*1.0/b.elapsed_time)*60 as numeric(15,2)) end end as MBpm, case b.total_netpaths when 0 then 0 else case b.elapsed_time when 0 then cast((b.total_netpaths*1.0/(b.elapsed_time+1))*60 as numeric(15,2)) else cast((b.total_netpaths*1.0/b.elapsed_time)*60 as numeric(15,2)) end end as fpm, case b.total_netpaths when 0 then 0 else case b.total_megs when 0 then 0 else cast((b.total_megs*1.0/b.total_netpaths) as numeric(15,2)) end end as aMBpf "
				;;
		         --local)
				SHOWLOCALBACKUPS=" and n.gcname is NULL "
				;;
		         --remote)
				SHOWLOCALBACKUPS=" and n.gcname is NOT NULL "
				;;
		         --bend)
				BEND=", to_timestamp(start_time+elapsed_time) as bend"
				;;
                          --csv)
				TOCSV="YES"
                                ;;
			--last24)
				LAST24=" and b.start_time > $(date +%s)-86400 "
				;;
                 	--lasth)
				INT="$(echo ${*} | cut -f `echo ${NUM}+1 | bc` -d ' ')"
				INT="$(echo "${INT}"*3600 | bc)"
				LAST24=" and b.start_time > $(date +%s)-${INT} "                                             
				;;
                 	--lastd)
				INT="$(echo ${*} | cut -f `echo ${NUM}+1 | bc` -d ' ')"
				INT="$(echo "${INT}"*86400 | bc)"
				LAST24=" and b.start_time > $(date +%s)-${INT} "                                             
				;;
			--noactive)
				NOACTIVE=" and (b.status & 200) != 0 "
				;;
			--cols)
				COLS="$(echo ${*} | cut -f `echo ${NUM}+1 | bc` -d ' ')"
				COLS1="select ${COLS} from ("
				COLS2=") tmp "
				;;
			--extra)
				EXTRA=" , $(echo ${*} | cut -f `echo ${NUM}+1 | bc` -d ' ')"
				NUM=`echo ${NUM}+1 | bc`
				;;
			--imported)
				IMPORTED=" , (b.properties &32) != 0 as imported "
				;;
			*)
				echo "${CURRENT}" | grep -q -- "^-"
				if [[ ${?} == 0 ]]; then
					error "${CURRENT} is not a flag for the --bhc command."
				fi
				;;
		esac
	done
	echo "${TYPELIST}" | grep -q 1038
	if [[ $? == 0 ]]; then
		NOMETA=" "
	fi
	if [[ "${TODOHEADING}" != "" ]]; then
		echo -e "${TODOHEADING}"
	fi
	SYSTEMNAME=""
	if [[ "${SYSTEMIDLIST}" != "" ]]; then
		SYSTEMNAME=" s.name as system, "
	fi
	QUERY="${COLS1} select ${SYSTEMNAME} b.backup_no as id, b.job_no as jno, n.node_no as nno, b.instance_id as iid, ${SHOWCLIENT}, case ((b.properties&128)!=0) when true then q.server_instance_name else i.key1 end as key1, case ((b.properties&128)!=0) when true then q.database_name else case when i.app_id in (50,51) then NULL else i.key2 end end as key2, to_timestamp(b.start_time) as bstart${BEND}, ${LAST} case ((b.properties&128)!=0) when true then case q.sql_type when 1 then 'LSFu' when 2 then 'LSDi' else to_char(q.sql_type, '9999') end else ${TYPECASE} end as type, b.status ${ENCRYPT}, b.tape_no, b.elapsed_time as secs, b.total_megs as megs, b.total_netpaths as files, ${SSTATUSCASE} as repl, (b.properties & 2048) != 0 as synth ${IMPORTED} ${SHOWEXTENDED} ${STATS} ${EXTRA} ${SHOWRET} from ${TABLE} join bp.nodes n on b.node_no = n.node_no join bp.application_instances i on b.instance_id = i.instance_id left join bp.systems s on n.system_id = s.system_id left join bp.sql_backups q on b.backup_no = q.sql_ref ${JOINRET} where 1=1 ${SYSTEMIDLIST} ${SHOWLOCALBACKUPS} ${CLIENTLIST} ${GCLIENTLIST} ${INSTANCELIST} ${TYPELIST} ${JOBSLIST} ${INSTANCEIDLIST} ${SOURCELIST} ${NOMETA} ${NOPURGE} ${NOACTIVE} ${CURRENTINSTANCES} ${NOSYNTH} ${BACKUPNOLIST} ${NEWER} ${LAST24} ${SORTCOLS} ${SORTORDER} ${LIMIT} ${COLS2} "
	debug "QUERY=${QUERY}"
	if [[ "${TOCSV}" == "YES" ]]; then
		psql bpdb -U postgres -A -F ',' -c "${QUERY}"
	else
		psql bpdb -U postgres -c "${QUERY}"
	fi
}

backup_history_item(){
        debug "backup_history_item"
	BACKUPNO="${2}"
	QUERY="select b.backup_no, m.messages, b.node_no, n.node_name, n.gcname, i.key1, i.key2, l.type, b.tape_no, b.job_no, to_timestamp(b.start_time) as start_time, to_timestamp(b.start_time+b.elapsed_time) as end_time, b.elapsed_time, b.properties, b.total_netpaths, b.total_megs, b.sync_status, ${SSTATUSCASE} as repl, b.status, b.exit_code, b.type, b.notbacked, b.is_verified, b.verify_after, b.x_command_short, b.pid, b.data_bkno, b.instance_id, b.legal_hold, case b.total_megs when 0 then 0 else case b.elapsed_time when 0 then cast((b.total_megs*1.0/(b.elapsed_time+1)) as numeric(15,2)) else cast((b.total_megs*1.0/b.elapsed_time) as numeric(15,2)) end end as MBps, case b.total_megs when 0 then 0 else case b.elapsed_time when 0 then cast((b.total_megs*1.0/(b.elapsed_time+1))*60 as numeric(15,2)) else cast((b.total_megs*1.0/b.elapsed_time)*60 as numeric(15,2)) end end as MBpm, case b.total_netpaths when 0 then 0 else case b.elapsed_time when 0 then cast((b.total_netpaths*1.0/(b.elapsed_time+1))*60 as numeric(15,2)) else cast((b.total_netpaths*1.0/b.elapsed_time)*60 as numeric(15,2)) end end as fpm, case b.total_netpaths when 0 then 0 else case b.total_megs when 0 then 0 else cast((b.total_megs*1.0/b.total_netpaths) as numeric(15,2)) end end as aMBpf from bp.backups b join bp.nodes n on b.node_no = n.node_no join bp.application_instances i on b.instance_id = i.instance_id join bp.application_lookup l on i.app_id = l.app_id left join bp.backup_msg m on b.backup_no = m.backup_no where b.backup_no = ${BACKUPNO}"
	debug "QUERY=${QUERY}"
	psql bpdb -U postgres -x -c "${QUERY}"

	QUERY="select properties from bp.backups where backup_no = ${BACKUPNO}"
	debug "QUERY=${QUERY}"
	PROPERTIES=`psql bpdb -U postgres -A -t -c "${QUERY}"`
	/usr/bp/bin/show_properties ${PROPERTIES}

	QUERY="select status from bp.backups where backup_no = ${BACKUPNO}"
	debug "QUERY=${QUERY}"
	STATUS=`psql bpdb -U postgres -A -t -c "${QUERY}"`
	/usr/bp/bin/show_status ${STATUS}
}

backup_search(){
	debug "backup_search"
        BACKUPLIST="${2}"
	SEARCHSTRING="${3}"
	if [[ "${BACKUPLIST}" == "" ]]; then
		error "A backup number or list was not specified."
	fi
	if [[ "${SEARCHSTRING}" == "" ]]; then
		error "A search string was not specified."
	fi
	QUERY="select f.backup_no, d.dname, f.fname, f.size as bytes, cast(((f.size*1.0)/1024) as numeric(10,2)) as kb, cast((((f.size*1.0)/1024)/1024) as numeric(10,2)) as mb, cast(((((f.size*1.0)/1024)/1024)/1024) as numeric(10,2)) as gb from bp.backup_files f join bp.dirs d on f.dir_no = d.dir_no where (d.dname ilike '%${SEARCHSTRING}' or f.fname ilike '${SEARCHSTRING}') and f.backup_no in (${BACKUPLIST}) order by f.backup_no, lower(d.dname), lower(f.fname), f.size asc"
	debug "QUERY=${QUERY}"
	psql bpdb -U postgres -c "${QUERY}"
}

tree_size(){
	debug "tree_size"
	TOTALSIZE=0
	TOTALITEMS=0
	shift
	BACKUPNO="${1}"
	shift
	STARTITEM="${*}"
	debug "STARTITEM=.${STARTITEM}."
	if [[ "${STARTITEM}" == "" ]]; then
		echo "No starting item was specified, here are the volumes that are options:"
		/usr/bp/bin/unitrends-cli get backups -z -i ${BACKUPNO} | grep '"name":' | awk -F '"' ' { print "\t"$4 } '
		exit
	fi
	# If *nix, add prefix
	echo "${STARTITEM}" | grep -q "^/"
	if [[ "${?}" == "0" ]]; then
		STARTITEM="@@@:${STARTITEM}"
	fi
	STARTITEM=`echo "${STARTITEM}" | sed "s/'/\'\'/g"`
	debug "STARTITEM=.${STARTITEM}."
	PARENTDIR="`dirname "${STARTITEM}"`/"
	CHILDDIR="`basename "${STARTITEM}"`"
	if [[ "${PARENTDIR}" == "./" ]]; then
		PARENTDIR="${CHILDDIR}"
		CHILDDIR="%"
	fi
	debug "PARENTDIR=.${PARENTDIR}."
	debug "CHILDDIR=.${CHILDDIR}."
	QUERY="select f.type from bp.dirs d join bp.backup_files f using(dir_no) where f.backup_no = ${BACKUPNO} and d.dname='${PARENTDIR}' and f.fname='${CHILDDIR}'"
	debug "QUERY=${QUERY}"
	STYPE="$(psql bpdb -U postgres -A -t -c "${QUERY}")"
	if [[ "${CHILDDIR}" == "%" ]]; then
		STYPE=53
	fi
	debug "STYPE=.${STYPE}."
	if [[ "${STYPE}" == "" ]]; then
		PARENTDIR=`echo "${PARENTDIR}" | sed "s/^@@@://"`
		error "Path provided, ${PARENTDIR}${CHILDDIR}, is not in backup ${BACKUPNO}."
	fi
        echo -e "      Num_Items        Size_MB\tPathname"
        echo "===================================================="
	if [[ "${STYPE}" == "53" || "${STYPE}" == "57" ]]; then
		# STARTIME is a directory
		# If necessary, add trailing /
		echo "${STARTITEM}" | grep -q "/$"
		if [[ "${?}" != "0" ]]; then
			STARTITEM="${STARTITEM}/"
		fi
		QUERY="select d.dname, f.fname, f.type from bp.dirs d join bp.backup_files f using(dir_no) where f.backup_no = ${BACKUPNO} and d.dname = '${STARTITEM}' order by lower(d.dname), lower(f.fname) asc"
		debug "QUERY=${QUERY}"
		IFS="
"
		for ROW in `psql bpdb -U postgres -A -t -F'|' -c "${QUERY}"`
		do
			IFS=" "
			DNAME=`echo "${ROW}" | awk -F '|' ' { print $1 } ' | sed "s/'/\'\'/g"`
			FNAME=`echo "${ROW}" | awk -F '|' ' { print $2 } ' | sed "s/'/\'\'/g"`
			FTYPE=`echo "${ROW}" | awk -F '|' ' { print $3 } '`
			debug "DNAME=.${DNAME}."
			debug "FNAME=.${FNAME}."
			debug "FTYPE=.${FTYPE}."
			if [[ "${FTYPE}" == "53" || "${FTYPE}" == "57" ]]; then
				# Is a directory
                                QUERY="select count(*) from bp.dirs d join bp.backup_files f using(dir_no) where f.backup_no = ${BACKUPNO} and d.dname like '${DNAME}${FNAME}/%'"
                                debug "QUERY=${QUERY}"
                                ITEMS=`psql bpdb -U postgres -A -t -c "${QUERY}"`
				if [[ "${ITEMS}" == "" ]]; then
					ITEMS=0
				fi
				debug "ITEMS=.${ITEMS}."
                                QUERY="select sum(f.size) from bp.dirs d join bp.backup_files f using(dir_no) where f.backup_no = ${BACKUPNO} and d.dname like '${DNAME}${FNAME}/%'"
                                debug "QUERY=${QUERY}"
                                SIZE=`psql bpdb -U postgres -A -t -c "${QUERY}"`
				if [[ "${SIZE}" == "" ]]; then
					SIZE=0
				fi
				debug "SIZE=.${SIZE}."
				TOTALITEMS=`echo ${TOTALITEMS}+${ITEMS} | bc`
                                TOTALSIZE=`echo | awk -v total="${TOTALSIZE}" -v fsize="${SIZE}" ' { printf "%f", total+fsize } '`
                                LINE=`echo "${DNAME}${FNAME}/" | sed "s/^@@@://; s:'':\':g"`
                                echo | awk -v line="${LINE}" -v count="${ITEMS}" -v size="${SIZE}" ' { printf "%15s%15.2f\t%-50s\n", count, size/1024/1024, line; } '
			else
				# Is a file
		                TOTALITEMS=`echo ${TOTALITEMS}+1 | bc`
				QUERY="select f.size from bp.dirs d join bp.backup_files f using(dir_no) where f.backup_no = ${BACKUPNO} and d.dname='${DNAME}' and f.fname='${FNAME}'"
		                debug "QUERY=${QUERY}"
		                SIZE=`psql bpdb -U postgres -A -t -c "${QUERY}"`
				debug "SIZE=.${SIZE}."
                		TOTALSIZE=`echo | awk -v total="${TOTALSIZE}" -v fsize="${SIZE}" ' { printf "%f", total+fsize } '`
		                LINE=`echo "${DNAME}${FNAME}" | sed "s/^@@@://; s:'':\':g"`
                		echo | awk -v line="${LINE}" -v count="1" -v size="${SIZE}" ' { printf "%15s%15.2f\t%-50s\n", count, size/1024/1024, line; } '
			fi
			IFS="
"
		done
		IFS=" "
	else
		# Is a file
		QUERY="select cast (f.size/1024/1024 as numeric(10,2)) from bp.dirs d join bp.backup_files f using(dir_no) where f.backup_no = ${BACKUPNO} and d.dname='${PARENTDIR}' and f.fname='${CHILDDIR}'"
		debug "QUERY=${QUERY}"
		SIZE="$(psql bpdb -U postgres -A -t -c "${QUERY}")"
		TOTALITEMS="1"
		TOTALSIZE="${SIZE}"
		LINE="$(echo "${PARENTDIR}${CHILDDIR}" | sed "s/^@@@://; s:'':\':g")"
		echo | awk -v line="${LINE}" -v count="1" -v size="${SIZE}" ' { printf "%15s%15s\t%-50s\n", count, size, line; } '
	fi
	echo "===================================================="
	TOTALSIZE=`echo | awk -v total="${TOTALSIZE}" ' { printf "%.2f", total/1024/1024 } '`
        echo "Total:    ${TOTALITEMS} items, ${TOTALSIZE} MB"
}

schedule_history(){
        debug "schedule_history"
	SCHEDULEID=""
	SCHDULENAME=""
	LAST24=""
	LIMIT=""
	for ARG in `echo ${*}`
	do
		case ${ARG} in
			--id)
				shift
				SCHEDULEID=" and h.schedule_id in (${1}) "
				;;
			--name)
				shift
				SCHEDULENAME="${*}"
				SCHEDULENAME="$(echo "${SCHEDULENAME}" | sed "s/,/','/g; s/^/'/g; s/$/'/g")"
				SCHEDULENAME=" and h.name in (${SCHEDULENAME}) "
				SCHEDULENAME="$(echo "${SCHEDULENAME}" | sed "s/ --last24//g; s/ --id//g")"
				;;
			--last24)
				LAST24=" and s.scheduled_start_time >= $(date +%s)-86400 "
				;;
			--limit)
				shift
				LIMIT=" limit ${*}"
				;;
			*)
				shift
				;;
		esac
	done
	debug "SCHEDULEID=${SCHEDULEID}"
	debug "SCHEDULENAME=${SCHEDULENAME}"
	QUERY="select s.schedule_id as sid, h.name, n.node_no as nno, a.instance_id as iid, n.node_name, a.key1, a.key2, to_timestamp(s.scheduled_start_time) as start, s.type as stype, ${STYPECASE} as stype, case when s.type = 13 then 'Arch' else ${TYPECASE} end as type, s.job_no, s.backup_no, b.elapsed_time as secs, b.total_megs as megs, b.total_netpaths as files, substring(s.status_string,0,30) as status_string from bp.schedule_history s join bp.application_instances a using(instance_id) join bp.nodes n using(node_no) left join bp.backups b using(backup_no) join bp.schedules h using(schedule_id) where 1=1 ${SCHEDULEID} ${SCHEDULENAME} ${LAST24} order by s.scheduled_start_time desc ${LIMIT}"
	debug "QUERY=${QUERY}"
	psql bpdb -U postgres -c "${QUERY}"
}

gantt_chart(){
        debug "gantt_chart"
	DATE="${2}"
	if [[ "${DATE}" == "" ]]; then
		DATE="$(date +%m/%d/%Y)"
	fi
	STARTDATE=`date -d "${DATE}" +%s 2> /dev/null`
	if [[ $? != 0 ]]; then
		error "bad date input"
	fi
	STARTDATE="$(date -d "$(date -d @"${STARTDATE}" +%m/%d/%Y)" +%s)"
	ENDDATE=`echo ${STARTDATE}+86400 | bc`
	applianceInfoHeader
	echo
	echo -e "Backups Gantt Chart for $(date -d @"${STARTDATE}" +%m/%d/%Y):"
	echo | awk -v printstring="0   1   2   3   4   5   6   7   8   9   10  11  12  13  14  15  16  17  18  19  20  21  22  23" -v node="client" -v backupno="backupno" -v btype="type" -v totalmegs="megs" -v bstatus="status" ' { printf "%s       %-48s %-10s %-6s %-10s %-6s\n", printstring, node, backupno, btype, totalmegs, bstatus } '
	echo "-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------"
	QUERY="select case when i.key4 is NOT NULL then n.node_name || '->' || i.key1 || '\\\' || i.key2 || '\\\' || i.key3 || '\\\' || i.key4 else case when i.key3 is NOT NULL then n.node_name || '->' || i.key1 || '\\\' || i.key2 || '\\\' || i.key3 else case when i.key2 is NOT NULL then n.node_name || '->' || i.key1 || '\\\' || i.key2 else case when i.key1 is NOT NULL then  n.node_name || '->' || i.key1 else n.node_name end end end end  as asset, ${TYPECASE}, b.start_time, b.elapsed_time, b.status, b.backup_no, b.total_megs from bp.backups b join bp.nodes n using (node_no) join bp.application_instances i using (instance_id) where ((b.start_time < ${STARTDATE} and b.start_time+b.elapsed_time > ${STARTDATE}) or (b.start_time >= ${STARTDATE} and b.start_time < ${ENDDATE})) and b.type in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,1024,1031,1032,1033,1034,1035,1036,1037,1039,1040,1041,4106,4107,4108) and b.x_command_short not like 'Sync%' and n.system_id is NULL order by b.backup_no asc"
	debug "QUERY=${QUERY}"
	IFS="
"
	for LINE in `psql bpdb -U postgres -A -t -F '|' -c "${QUERY}"`
	do
		IFS=" "
		NODE=`echo ${LINE} | awk -F '|' ' { print $1 } '`
		TYPE=`echo ${LINE} | awk -F '|' ' { print $2 } '`
		START=`echo ${LINE} | awk -F '|' ' { print $3 } '`
		ELAPSED=`echo ${LINE} | awk -F '|' ' { print $4 } '`
		STATUS=`echo ${LINE} | awk -F '|' ' { print $5 } '`
		BACKUPNO=`echo ${LINE} | awk -F '|' ' { print $6 } '`
		TOTALMEGS=`echo ${LINE} | awk -F '|' ' { print $7 } '`
		STARTTIME=`echo ${LINE} | awk -F '|' ' { print $8 } '`
		LPAD=0
		DURATION=0
		RPAD=0
	
		# Calculate leading spaces."
		if [[ "${START}" -gt "${STARTDATE}" ]]; then
			LPAD=`echo "(((${START}-${STARTDATE})/60)/15)" | bc`
		fi
		debug "LPAD=.${LPAD}."
		
		# Calculate duration hashes."
		DURATION=`echo "((${ELAPSED}/60)/15)" | bc`
		TMP=`echo "(${LPAD}+${DURATION})" | bc`
		if [[ "${TMP}" -gt "96" ]]; then
			DURATION=`echo "(96-${LPAD})" | bc`
		fi
		if [[ "${DURATION}" == "0" ]]; then
			DURATION=1
		fi
		debug "DURATION=.${DURATION}."
	
		# Calculate trailing spaces."
		TMP=`echo "(${LPAD}+${DURATION})" | bc`
		if [[ "${TMP}" -lt "96" ]]; then
			RPAD=`echo "(96-${TMP})" | bc`
		fi
		debug "RPAD=.${RPAD}."
	
		# Print to stdout.
		unset IFS
		PRINTSTRING=""
		for NUM in `seq ${LPAD}`; do PRINTSTRING="${PRINTSTRING} "; done
		if [[ "${STATUS}" == "3168" ]]; then
			for NUM in `seq ${DURATION}`; do PRINTSTRING="${PRINTSTRING}#"; done
		elif [[ "${STATUS}" == "19552" ]]; then
			for NUM in `seq ${DURATION}`; do PRINTSTRING="${PRINTSTRING}!"; done
		elif [[ "${STATUS}" == "512" || "${STATUS}" == "514" || "${STATUS}" == "524800" ]]; then
			for NUM in `seq ${DURATION}`; do PRINTSTRING="${PRINTSTRING}A"; done
		else
			for NUM in `seq ${DURATION}`; do PRINTSTRING="X"; done
		fi
		for NUM in `seq ${RPAD}`; do PRINTSTRING="${PRINTSTRING} "; done
		echo | awk -v printstring="${PRINTSTRING}" -v node="${NODE}" -v backupno="${BACKUPNO}" -v btype="${TYPE}" -v totalmegs="${TOTALMEGS}" -v bstatus="${STATUS}" ' { printf "%s     %-48s %-10d %-6s %-10d %-6d\n", printstring, node, backupno, btype, totalmegs, bstatus } '
		IFS="
"
	done
	IFS=" "
}

jobs(){
        debug "jobs"
	applianceInfoHeader
	echo
	QUERY="select j.job_no as jno, j.instance_id as iid, case when n.gcname is NOT NULL then n.gcname else n.node_name end as client, key1, case when b.type in (1031,1032,1033,16,96) then '' else key2 end as key2, to_timestamp(queued_date) as queued, case when j.start_time <> 0 then to_timestamp(j.start_time) else NULL end as start, case j.pid when 0 then NULL else j.pid end as pid, case j.backup_no when 0 then NULL else j.backup_no end as backup_no, case when b.status is NULL then NULL else b.status end as status, case when b.type is NULL then NULL else ${TYPECASE} end as type, percent_done as pct, comment from bp.jobs j join bp.nodes n using(node_no) join bp.application_instances a using(instance_id) left join bp.backups b using(backup_no) order by j.job_no asc"
	debug "QUERY=${QUERY}"
	psql bpdb -U postgres -c "${QUERY}"
}

watch_jobs(){
	debug "watch_jobs"
	if [[ "$(rosVersion)" == "7" ]]; then
		watch -d -n 2 --color "sh ${0} --jobs"
	else
		watch -d -n 2 "sh ${0} --jobs"
	fi
}

get_client_logs(){
	debug "get_client_logs"
	TYPE="${1}"
	CLIENT="${2}"
	if [[ "${CLIENT}" == "" ]]; then
		error "Need to specify a client name."
	fi
	GETLOGS=""
	if [[ "${3}" == "evtx" ]]; then
		GETLOGS="|/Windows/System32/winevt/Logs"
	fi
	QUERY="select count(*) from bp.nodes where node_name = '${CLIENT}'"
	debug "QUERY=${QUERY}"
	RET="$(psql bpdb -U postgres -A -t -c "${QUERY}")"
	if [[ "${RET}" != "1" ]]; then
		error "${CLIENT} is not a client to this appliance."
	fi
	case ${TYPE} in
		win)
	                debug "A Windows clientwas specified."
			QUERY="select regexp_replace(version, '[^0-9]+', '', 'g') from bp.nodes where node_name = '${CLIENT}'"
			debug "QUERY=${QUERY}"
			AGENTVERSION="$(psql bpdb -U postgres -A -t -c "${QUERY}")"
			QUERY="select count(*) from bp.nodes n join bp.application_instances i using (node_no) join bp.application_lookup l using(app_id) where l.type = 'Hyper-V' and n.node_name = '${CLIENT}'"
			debug "QUERY=${QUERY}"
		        NUM=$(psql bpdb -U postgres -A -t -c "${QUERY}")
			if [[ "${AGENTVERSION}" -ge "1033" ]]; then
				QUERY="select node_no from bp.nodes where node_name = '${CLIENT}'"
				debug "QUERY=${QUERY}"
				NNO="$(psql bpdb -U postgres -A -t -c "${QUERY}")"
				QUERY="select system_id from bp.systems where is_local_system = true"
				debug "QUERY=${QUERY}"
				SID="$(psql bpdb -U postgres -A -t -c "${QUERY}")"
				UVCBT=""
				/usr/bp/bin/unitrends-cli get clients --files --sid ${SID} --id ${NNO} --dir C: | grep -q '"id": "C:/Unitrendsvcbt/"'
				if [[ ${?} == 0 ]]; then
					UVCBT="\"C:/Unitrendsvcbt\","
				fi
				QUERY="select instance_id from bp.application_instances where app_id = 1 and node_no in (select node_no from bp.nodes where node_name = '${CLIENT}')"
				debug "QUERY=${QUERY}"
				IID="$(psql bpdb -U postgres -A -t -c "${QUERY}")"
				if [[ "${NUM}" -gt "0" ]]; then
                                        # Found Hyper-V application for node_name
					if [[ "${GETLOGS}" == "" ]]; then
						/usr/bp/bin/unitrends-cli PUT backups -s 1 -R ' { "instances" : [ '${IID}' ], "backup_type" : "Selective", "storage": "internal", "verify_level" : "none", "incl_list" : [ '${UVCBT}' "C:/PCBP/Catalog.dir", "C:/PCBP/Logs.dir", "C:/PCBP/MASTER.INI", "C:/PCBP/Lists.dir", "C:/PCBP/Info.dir", "C:/unicbt" ] } '
					else
						/usr/bp/bin/unitrends-cli PUT backups -s 1 -R ' { "instances" : [ '${IID}' ], "backup_type" : "Selective", "storage": "internal", "verify_level" : "none", "incl_list" : [ '${UVCBT}' "C:/PCBP/Catalog.dir", "C:/PCBP/Logs.dir", "C:/PCBP/MASTER.INI", "C:/PCBP/Lists.dir", "C:/PCBP/Info.dir", "C:/Windows/System32/winevt/Logs", "C:/unicbt" ] } '
					fi
				else
					# No Hyper-V found for node_name
					if [[ "${GETLOGS}" == "" ]]; then
						/usr/bp/bin/unitrends-cli PUT backups -s 1 -R ' { "instances" : [ '${IID}' ], "backup_type" : "Selective", "storage": "internal", "verify_level" : "none", "incl_list" : [ '${UVCBT}' "C:/PCBP/Catalog.dir", "C:/PCBP/Logs.dir", "C:/PCBP/MASTER.INI", "C:/PCBP/Lists.dir", "C:/PCBP/Info.dir" ] } '
					else
						/usr/bp/bin/unitrends-cli PUT backups -s 1 -R ' { "instances" : [ '${IID}' ], "backup_type" : "Selective", "storage": "internal", "verify_level" : "none", "incl_list" : [ '${UVCBT}' "C:/PCBP/Catalog.dir", "C:/PCBP/Logs.dir", "C:/PCBP/MASTER.INI", "C:/PCBP/Lists.dir", "C:/PCBP/Info.dir", "C:/Windows/System32/winevt/Logs" ] } '
					fi
				fi
			else
		                if [[ "${NUM}" -gt "0" ]]; then
					# Found Hyper-V application for node_name
		                        /usr/bp/bin/bpr -cVbf 20 D2DBackups -zWHERE=C:/ -zSTATION=${CLIENT} "C:/PCBP/Logs.dir|/PCBP/Catalog.dir|/PCBP/Lists.dir|/PCBP/Info.dir|/PCBP/MASTER.INI|/unicbt${GETLOGS}"
		                else
					# No Hyper-V found for node_name
		                        /usr/bp/bin/bpr -cVbf 20 D2DBackups -zWHERE=C:/ -zSTATION=${CLIENT} "C:/PCBP/Logs.dir|/PCBP/Catalog.dir|/PCBP/Lists.dir|/PCBP/Info.dir|/PCBP/MASTER.INI${GETLOGS}"
		                fi
			fi
			;;
		lin)
			debug "A *nix specified."
	                /usr/bp/bin/bpr -cVbf 20 D2DBackups -zWHERE=/ -zSTATION=${CLIENT} '/usr/bp/bpinit/master.ini|/usr/bp/logs.dir|/usr/bp/catalog.dir|/usr/bp/incremental_forever/journal.bak|/usr/bp/incremental_forever/journal.excludes|/usr/bp/bp_VERS'
			;;
		*)
			error "Client OS type ($TYPE} is not valid.  Use either win or lin."
			;;
	esac
}

pids(){
        debug "pids"
	QUERY="select pid, filename, comment, to_timestamp(time_stamp) as time_stamp from bp.pids order by filename asc"
	debug "QUERY=${QUERY}"
	psql bpdb -U postgres -c "${QUERY}"
}

replication_watch_dashboard(){
        debug "replication_watch_dashboard"
	if [[ "$(rosVersion)" == "7" ]]; then
		watch -n 5 -d --color "sh ${0} --rdash ${1} 2>&1"
	else
		watch -n 5 -d "sh ${0} --rdash ${1} 2>&1"
	fi
}

replication_dashboard(){
        debug "replication_dashboard"
	ROWS=${1:-10}
	applianceInfoHeader
	echo
	replication_active
        echo
	replication_history ${ROWS}
	echo
	replication_queue ${ROWS}
        echo
        replication_pids
}

replication_active(){
        debug "replication_active"
	echo -e "MaxConcurrent=\c"
        # Get version, such that 10.4.0-2 becomes 010004000001
        TMP="$(dpu version | grep "^Version" | awk ' { print $2 } ' | awk -F "[.-]" ' { printf("%03d", $1); printf("%03d", $2); printf("%03d", $3); printf("%03d", $4) } ')"
        if [[ "10#${TMP}" -ge "10#010004000001" ]]; then
		echo $(psql bpdb -U postgres -c "select max_concurrent from bp.source_replication_config order by target_id  asc" -A -t ) | sed "s/ /, /g"
	else
		/usr/bp/bin/bputil -g "Replication" "MaxConcurrent" "-1" /usr/bp/bpinit/master.ini
	fi
	QUERY="select cast (sum(kbps)*8.0/1024 as numeric(15,2)) from (select name_value as name, substring(to_timestamp(bp.operation_progress.start_time)::VARCHAR,6,14) as start, substring(to_timestamp(snapshot_time)::VARCHAR,6,14) as snap, final_size as final_mb, current_size as current_mb,bp.backups.total_netpaths as num_files, pid_to_kill as pid, on_behalf_of as client, bp.operation_progress.backup_no, backup_type as type, (current_size*100)/(final_size+1) as percent, (current_size*1024)/((snapshot_time-bp.operation_progress.start_time)+1) as kbps, to_timestamp(snapshot_time+(((final_size-current_size)*1024)/(((current_size*1024)/((snapshot_time-bp.operation_progress.start_time)+1)+1)))) as estimate, destination as target from bp.operation_progress join bp.operation_names on bp.operation_progress.name_id = bp.operation_names.name_id join bp.backups on bp.operation_progress.backup_no = bp.backups.backup_no where bp.operation_progress.name_id not in (11,13)) as foo"
	debug "QUERY=${QUERY}"
	ACTIVE="$(psql bpdb -U postgres -A -t -c "${QUERY}")"
        echo Active:  ${ACTIVE} e_Mbps | sed "s/Active: e_Mbps/Active: 0 e_Mbps/"
	QUERY="select o.name_value as action, substring(to_timestamp(p.start_time)::VARCHAR,0,17) as rstart, substring(to_timestamp(p.snapshot_time)::VARCHAR,0,17) as rsnap, cast(((p.start_time-(b.start_time+b.elapsed_time))*1.0/86400) as numeric(15,2)) as rwait,  cast(((p.snapshot_time-p.start_time)*1.0)/86400 as numeric(15,2)) as rdur, final_size as final_mb, current_size as current_mb, b.total_netpaths as files, p.pid_to_kill as pid, i.instance_id as iid, case when n.system_id is NOT NULL then n.gcname else n.node_name end as client, i.key1, case when b.type in (1031,1032,1033) then '' else i.key2 end as key2, q.backup_no, ${TYPECASE} as type, (p.current_size*100)/(p.final_size+1) as percent, cast ((p.current_size*8.0)/((p.snapshot_time-p.start_time)+1) as numeric(15,2)) as e_Mbps, substring(to_timestamp(p.snapshot_time+(((p.final_size-p.current_size)*1024)/(((p.current_size*1024)/((p.snapshot_time-p.start_time)+1)+1))))::VARCHAR,0,17) as estimate, (b.properties & 2048) != 0 as synth, destination as target from bp.replication_queue q left join bp.operation_progress p on q.backup_no = p.backup_no left join bp.operation_names o on p.name_id = o.name_id join bp.backups b on q.backup_no = b.backup_no join bp.application_instances i on b.instance_id = i.instance_id join bp.nodes n on i.node_no = n.node_no where q.status = 16 order by q.position desc"
	debug "QUERY=${QUERY}"
	psql bpdb -U postgres -c "${QUERY}"
}

replication_history(){
        debug "replication_history"
	LIMIT=${1:-9999}
        echo -e "History:  \c"

	QUERY="select count(*) from bp.sds_stats where done_epoch >= ($(date +%s)-86400)"
	debug "QUERY=${QUERY}"
        echo -e "$(psql bpdb -U postgres -A -t -c "${QUERY}") completed last 24 hours, \c"

	QUERY="select trim(both ' ' from to_char(cast(((sum(bytes_on_dpv)*1.0/1024)/1024) as numeric(15,2)), '99,999,999,999.99')) from bp.sds_stats where done_epoch >= ($(date +%s)-86400)"
	debug "QUERY=${QUERY}"
        echo -e "$(psql bpdb -U postgres -A -t -c "${QUERY}") eMBsyncd, \c"

	QUERY="select trim(both ' ' from to_char(cast(((sum(bytes_syncd)*1.0/1024)/1024) as numeric(15,2)), '99,999,999,999.99')) from bp.sds_stats where done_epoch >= ($(date +%s)-86400)"
	debug "QUERY=${QUERY}"
        echo -e "$(psql bpdb -U postgres -A -t -c "${QUERY}") aMBsyncd, \c"

	QUERY="select count(*) from bp.sds_stats where message ilike 'success%' and done_epoch > $(date +%s)-86400"
	debug "QUERY=${QUERY}"
	SUCCESSFUL="$(psql bpdb -U postgres -A -t -c "${QUERY}")"

	QUERY="select count(*) from bp.sds_stats where message not ilike 'success%' and done_epoch > $(date +%s)-86400"
	debug "QUERY=${QUERY}"
	FAILED="$(psql bpdb -U postgres -A -t -c "${QUERY}")"
	echo -e "${SUCCESSFUL} successful, ${FAILED} failed"

	QUERY="select bp.sds_stats.backup_no, i.instance_id as iid, client_name, key1, case when b.type in (1031,1032,1033) then '' else key2 end as key2, ${TYPECASE} as type, substring(to_timestamp(b.start_time+b.elapsed_time)::VARCHAR,0,17) as bend, substring(to_timestamp(done_epoch)::VARCHAR,0,17) as rend, presync_seconds+sync_seconds+postsync_seconds as secs, total_megs megs, case when bytes_syncd = 0 then 0 else cast(((bytes_syncd*1.0/1024)/1024) as numeric(15,2)) end as syncd, case when bytes_on_dpu = 0 then 0 else cast((((bytes_on_dpu*8.0/1024/1024))/((presync_seconds+sync_seconds+postsync_seconds+1))) as numeric(15,2)) end as e_mbps, case when bytes_syncd = 0 then 0 else cast((((bytes_syncd*8.0/1024/1024))/((presync_seconds+sync_seconds+postsync_seconds+1))) as numeric(15,2)) end as a_mbps, case when b.type = 1038 then 100 else case when bytes_syncd = 0 then 0 else (bytes_syncd*100)/(bytes_on_dpu+1) end end as delta, case when message like 'Success%' then 'Success' when message like 'CANCELLED%' then 'Canceled' else 'Failed' end as status, (b.properties & 2048) != 0 as synth, cast((done_epoch-(presync_seconds+sync_seconds+postsync_seconds)-(b.start_time+b.elapsed_time))*1.0/86400 as numeric(15,2)) as rwait, cast((presync_seconds+sync_seconds+postsync_seconds)*1.0/86400 as numeric(15,2)) as rdur, cast((done_epoch-(b.start_time+b.elapsed_time))*1.0/86400 as numeric(15,2)) as roffsite, destination as target from bp.backups b right join bp.sds_stats on b.backup_no = bp.sds_stats.backup_no join bp.application_instances i on b.instance_id = i.instance_id where 1=1 order by done_epoch desc limit ${LIMIT}"
	debug "QUERY=${QUERY}"
        psql bpdb -U postgres -c "${QUERY}" | sed "s_| Failed: F |_| Failed:   |_g"
}

replication_queue(){
        debug "replication_queue"
	LIMIT=${1:-9999}
	QUERY="select count(*) from bp.replication_queue where status = 1"
	debug "QUERY=${QUERY}"
        PENDING="$(psql bpdb -U postgres -A -t -c "${QUERY}")"
        echo -e "Pending:  ${PENDING}"

	QUERY="select count(distinct(b.instance_id)) from bp.replication_queue q join bp.backups b using(backup_no) where q.status = 1"
	debug "QUERY=${QUERY}"
	NUMINSTANCE="$(psql bpdb -U postgres -A -t -c "${QUERY}")"
	echo -e "Instances: ${NUMINSTANCE}"

	QUERY="select case (select count(*) from bp.replication_queue where status = 1) when 0 then '0' else trim(both ' ' from to_char(sum(total_megs), '9,999,999,999,999,999')) end from bp.backups b join bp.replication_queue q  using(backup_no) where q.status = 1"
	debug "QUERY=${QUERY}"
	NSIZEQUEUE="$(psql bpdb -U postgres -A -t -c "${QUERY}")"
	echo -e "Queue NSize: ${NSIZEQUEUE} MB"

	QUERY="select count(*) from bp.replication_queue where status = 128"
	debug "QUERY=${QUERY}"
	NPAUSED="$(psql bpdb -U postgres -A -t -c "${QUERY}")"
	echo -e "Num Paused Instances: ${NPAUSED}"

	QUERY="select row_number() over (order by position asc), position, b.backup_no, i.instance_id as iid, case when system_id is NOT NULL then gcname else node_name end as client, key1, case when b.type in (1031,1032,1033) then '' else key2 end as key2, ${TYPECASE} as type, substring(to_timestamp(b.start_time)::VARCHAR,0,17) as bstart, substring(to_timestamp(b.start_time+b.elapsed_time)::VARCHAR,0,17) as bend, total_netpaths as files, total_megs as megs, (b.properties & 2048) != 0 as synth, vault as target, cast(($(date +%s)-(b.start_time+b.elapsed_time))*1.0/86400 as numeric(15,2)) as rwait from bp.backups b join bp.replication_queue on b.backup_no = bp.replication_queue.backup_no join bp.application_instances i on b.instance_id = i.instance_id join bp.nodes on b.node_no = bp.nodes.node_no  where bp.replication_queue.status = 1 order by position asc limit ${LIMIT}"
	debug "QUERY=${QUERY}"
        psql bpdb -U postgres -c "${QUERY}"
}

replication_statistics(){
	debug "replication_statistics"
	echo "$(hostname) ($(/usr/local/bin/dpu asset | grep "^DPU" | awk ' { print $NF } ')) - Replication statistics for $(date +%m/%d | sed "s/^[0]*//g"):"
	echo "-----------------------------------------------------------------------------------------------"

	# Get version, such that 10.4.0-2 becomes 010004000001
	TMP="$(dpu version | grep "^Version" | awk ' { print $2 } ' | awk -F "[.-]" ' { printf("%03d", $1); printf("%03d", $2); printf("%03d", $3); printf("%03d", $4) } ')"
	if [[ "10#${TMP}" -ge "10#010004000001" ]]; then
		psql bpdb -U postgres -c "select count(*) from bp.source_replication_config" -A -t | grep -q "^0$"
		if [[ ${?} == 0 ]]; then
			echo "Replication is not enabled."
			exit 0
		fi
	else
		/usr/bp/bin/bputil -g "Replication" "Enabled" "no" /usr/bp/bpinit/master.ini | grep -q "no"
		if [[ $? == 0 ]]; then
			echo "Replication is not enabled."
			exit 0
		fi
	fi
	QUERY="select count(*) from bp.replication_queue q left join bp.operation_progress p on q.backup_no = p.backup_no left join bp.operation_names o on p.name_id = o.name_id join bp.backups b on q.backup_no = b.backup_no join bp.application_instances i on b.instance_id = i.instance_id join bp.nodes n on i.node_no = n.node_no where q.status = 16"
	debug "QUERY=${QUERY}"
	ACTIVE="$(psql bpdb -U postgres -A -t -c "${QUERY}")"
	echo -e "${ACTIVE} active"

	QUERY="select count(*) from bp.replication_queue where status = 1"
	debug "QUERY=${QUERY}"
	echo -e "$(psql bpdb -U postgres -A -t -c "${QUERY}") pending \c"

	QUERY="select case (select count(*) from bp.replication_queue where status = 1) when 0 then '0' else trim(both ' ' from to_char(sum(total_megs), '9,999,999,999,999,999')) end from bp.backups b join bp.replication_queue q  using(backup_no) where q.status = 1"
	debug "QUERY=${QUERY}"
	NATIVE="$(psql bpdb -U postgres -A -t -c "${QUERY}")"
	echo -e "(${NATIVE} MB native)"

	QUERY="select count(*) from bp.sds_stats where done_epoch >= ($(date +%s)-86400)"
	debug "QUERY=${QUERY}"
	LAST24="$(psql bpdb -U postgres -A -t -c "${QUERY}")"
	echo -e "${LAST24} completed last 24 hours (\c"

	QUERY="select count(*) from bp.sds_stats where message ilike 'success%' and done_epoch > $(date +%s)-86400"
	debug "QUERY=${QUERY}"
	SUCCESSFUL="$(psql bpdb -U postgres -A -t -c "${QUERY}")"
	echo -e "${SUCCESSFUL} successful, \c"

	QUERY="select count(*) from bp.sds_stats where message not ilike 'success%' and done_epoch > $(date +%s)-86400"
	debug "QUERY=${QUERY}"
	FAILED="$(psql bpdb -U postgres -A -t -c "${QUERY}")"
	echo -e "${FAILED} failed)"

	QUERY="select coalesce(trim(both ' ' from to_char(cast(((sum(bytes_on_dpv)*1.0/1024/1024)/1024) as numeric(15,2)), '99,999,999,999.99')), '0') from bp.sds_stats where done_epoch >= ($(date +%s)-86400)"
	debug "QUERY=${QUERY}"
	EFFECTIVE="$(psql bpdb -U postgres -A -t -c "${QUERY}")"
        echo -e "${EFFECTIVE} GB effectively syncd; \c"

        QUERY="select coalesce(trim(both ' ' from to_char(cast(((sum(bytes_syncd)*1.0/1024/1024)/1024) as numeric(15,2)), '99,999,999,999.99')), '0') from bp.sds_stats where done_epoch >= ($(date +%s)-86400)"
	debug "QUERY=${QUERY}"
	ACTUAL="$(psql bpdb -U postgres -A -t -c "${QUERY}")"
        echo -e "${ACTUAL} GB actually syncd"

	QUERY="select count(distinct(instance_id)) from bp.replication_queue q join bp.backups b using(backup_no) where q.status = 128"
	debug "QUERY=${QUERY}"
	PAUSED="$(psql bpdb -U postgres -A -t -c "${QUERY}")"
	echo -e "${PAUSED} paused instances"
}

replication_paused(){
	debug "replication_paused"
	QUERY="select * from bp.replication_queue where status = 128"
	debug "QUERY=${QUERY}"
	psql bpdb -U postgres -c "${QUERY}"
}

replication_pids(){
        debug "replication_pids"
	echo -e "PIDs:  \c"
	NUMVCD="$(echo `ps -eo pid,etime,args | egrep "/usr/bp/bin/vcd" | grep -v grep | wc -l`-1 | bc | sed "s/-1/0/g")"
        echo " ${NUMVCD} vcd workers"
	NUMVAULTSERVER="$(echo `ps -eo pid,etime,args | egrep "/var/www/cgi-bin/vaultServer" | grep -v "grep" | wc -l` | grep -v grep | sed "s/-1/0/g")"
        echo -e "\t${NUMVAULTSERVER} vaultServer workers"
        echo
	ps -o pid,ppid,etime,args -C vcd,vaultServer,inlineHashdb,hactarToctar,recipe
        echo
}

replication_config(){
        debug "replication_config"
	/usr/bp/bin/bputil -g "Replication" "NULL" "NULL" /usr/bp/bpinit/master.ini
	/usr/bp/bin/bputil -g "CMC" "NULL" "NULL" /usr/bp/bpinit/master.ini | egrep "\[CMC\]|VaultConnection|VaultUpstreamSpeed|VaultDownstreamSpeed|VaultBandwidth"
	/usr/bp/bin/bputil -g "debugging" "NULL" "NULL" /usr/bp/bpinit/master.ini | egrep "\[debugging\]|vcd|vaultServer"

	# Get version, such that 10.4.0-2 becomes 010004000001
        TMP="$(dpu version | grep "^Version" | awk ' { print $2 } ' | awk -F "[.-]" ' { printf("%03d", $1); printf("%03d", $2); printf("%03d", $3); printf("%03d", $4) } ')"
	if [[ "10#${TMP}" -ge "10#010004000001" ]]; then
		echo
		echo "Source:"
		psql bpdb -U postgres -c "select $(echo $(psql bpdb -U postgres -c "SELECT column_name FROM information_schema.columns WHERE table_schema = 'bp' AND table_name = 'source_replication_config' ORDER BY column_name ASC;" -A -t) | sed "s/ /, /g") from bp.source_replication_config" -x
		echo
		echo "Target:"
		psql bpdb -U postgres -c "select $(echo $(psql bpdb -U postgres -c "SELECT column_name FROM information_schema.columns WHERE table_schema = 'bp' AND table_name = 'target_replication_config' ORDER BY column_name ASC;" -A -t) | sed "s/ /, /g") from bp.target_replication_config" -x
	fi

	echo
	echo "Throttle settings:"
	THROTTLE="$(/usr/bp/bin/bputil -g "Replication" "BlockOutPeriods" "lola" /usr/bp/bpinit/master.ini)"
	# Nowa
	CURRENT=$(date +%k)
	TODAYS="$(echo "${THROTTLE}" | sed "s/,/\n/g" | grep "^$(date +%u):")"
	for ENTRY in `echo ${TODAYS}`
	do
		START=$(echo "${ENTRY}" | awk -F ':' ' { print $2 } ' | awk -F '=' ' { print $1 } ' | awk -F '-' ' { print $1 } ')
		END=$(echo "${ENTRY}" | awk -F ':' ' { print $2 } ' | awk -F '=' ' { print $1 } ' | awk -F '-' ' { print $2 } ')
		if [[ "${START}" -le "${CURRENT}" && "${END}" -ge "${CURRENT}" ]]; then
			echo -e "\tCurrent:  $(echo "scale=2; $(echo "${ENTRY}" | awk -F '=' ' { print $2 } ' | sed "s/[a-zA-Z]//g")/125" | bc) Mbps"
		fi
	done

	# By day
	WTOTAL=0
	for DAY in `seq 0 6`
	do
		TOTAL=0
		for ENTRY in `echo "${THROTTLE}" | sed "s/,/\n/g" | grep "^${DAY}:" | sort`
		do
			START=$(echo "${ENTRY}" | awk -F ':' ' { print $2 } ' | awk -F '=' ' { print $1 } ' | awk -F '-' ' { print $1 } ')
	                END=$(echo "${ENTRY}" | awk -F ':' ' { print $2 } ' | awk -F '=' ' { print $1 } ' | awk -F '-' ' { print $2 } ')
			for NUM in `seq ${START} ${END}`
			do
				CURRENT=$(echo "scale=2; $(echo "${ENTRY}" | awk -F '=' ' { print $2 } ' | sed "s/[a-zA-Z]//g")/125" | bc)
				TOTAL=$(echo "${TOTAL}+${CURRENT}" | bc)
			done
		done
		TOTAL=$(echo "scale=2; ${TOTAL}/24" | bc)
		WTOTAL=$(echo "${WTOTAL}+${TOTAL}" | bc)
		echo -e "\tDay ${DAY} average:  ${TOTAL} Mbps"
	done

	# For week
	WTOTAL=$(echo "scale=2; ${WTOTAL}/7" | bc)
	echo -e "\tWeek average:  ${WTOTAL} Mbps"
}

replication_log(){
        debug "replication_log"
	CMD="vi"
        if [[ "${3}" == "tail" ]]; then
                CMD="tail -n 70 -F"
        fi
        ps -eo pid,args | grep -v grep | grep -q "${2}.*vaultServer"
        if [[ ${?} == 0 ]]; then
                ${CMD} /proc/${2}/fd/`ls -la /proc/${2}/fd | grep vaultServer.*log | awk ' { print $9 } '`
        fi
        ps -eo pid,args | grep -v grep | grep -q "${2}.*vcd"
        if [[ ${?} == 0 ]]; then
                ${CMD} /proc/${2}/fd/`ls -la /proc/${2}/fd | grep vcd.*log | awk ' { print $9 } '`
        fi
}

replication_history_delete(){
	debug "replication_history_delete"
	if [[ "${2}" == "" ]]; then
		error "replication_history_delete requires an argument."
	fi
	case ${2} in 
		all)
			QUERY="delete from bp.sds_stats"
			;;
		failed)
			QUERY="delete from bp.sds_stats where message like 'Fail%' and backup_no in (select backup_no from bp.sds_stats where message like 'Success%')"
			;;
		all_failed)
			QUERY="delete from bp.sds_stats where message like 'Fail%'"
			;;
		*)
			error "replication_history_delete requires a valid argument argument (all, failed, or all_failed)."
			;;
	esac
	debug "QUERY=${QUERY}"
	psql bpdb -U postgres -c "${QUERY}"
}

backup_extract(){
        debug "backup_extract"
	NUM="${2}"
	if [[ "${2}" == "clean" ]]; then
		echo "Removing /backups/samba/support_restore/"
		cd /backups/samba/ 2> /dev/null && rm -rf support_restore 2> /dev/null
		return
	fi
	QUERY="select dev_rw_name from bp.devices where is_scsi = false and tape_no in (select tape_no from bp.backups where backup_no = ${NUM})"
	debug "QUERY=${QUERY}"
        BPATH="$(psql bpdb -U postgres -A -t -c "${QUERY}")"
        BPATH="${BPATH}backup_${NUM}"
        RPATH="/backups/samba/support_restore/${NUM}/"
        INFILE="/tmp/infile.txt"
        mkdir -p "${RPATH}"
        echo "Restoring from backup #${NUM}."
        if [[ -f "${INFILE}" ]]; then
                EXTRAOPTS=" -F ${INFILE} "
                echo "Using inclusion file, ${INFILE}"
        else
                echo "Not using inclusion file, ${INFILE}"
        fi
        debug "/usr/bp/bin/fileDedup -R ""${BPATH}"" | /usr/bp/bin/bpcrypt -d | /usr/bp/bin/Bkup -xv -zWHERE=${RPATH} -zNOCNVT -f - ${EXTRAOPTS}"
        /usr/bp/bin/fileDedup -R "${BPATH}" | /usr/bp/bin/bpcrypt -d | /usr/bp/bin/Bkup -xv -zWHERE=${RPATH} -zNOCNVT -f - ${EXTRAOPTS}
	echo
}

backup_updatedb(){
        debug "backup_updatedb"
	NUM="${2}"
	QUERY="select dev_rw_name from bp.devices where is_scsi = false and tape_no in (select tape_no from bp.backups where backup_no = ${NUM})"
	debug "QUERY=${QUERY}"
        BPATH="$(psql bpdb -U postgres -A -t -c "${QUERY}")"
        BPATH="${BPATH}backup_${NUM}"
        debug "/usr/bp/bin/fileDedup -R ""${BPATH}"" | /usr/bp/bin/bpcrypt -d | /usr/bp/bin/updatedb -b ${NUM} -p -"
        /usr/bp/bin/fileDedup -R "${BPATH}" | /usr/bp/bin/bpcrypt -d | /usr/bp/bin/updatedb -b ${NUM} -p -
}

rpms(){
        debug "rpms"
	for RPM in `rpm -qa | grep unitrends`
	do
		echo "${RPM}:"
		rpm -V ${RPM}
	done
}

show_table(){
	debug "show_table"
	shift
	ARGTABLE="${1}"
	TABLE=`echo "${ARGTABLE}" | sed "s/^bp\.//g" | sed "s/^/bp\./g"`
	echo "${TABLE}" | grep -q "^${TABLE}$"
	if [[ ${?} != 0 ]]; then
		error "Table ${ARGTABLE} does not exist."
	fi
	debug "TABLE=${TABLE}"
	EXPAND=""
	echo "${*}" | grep -q -- -x
	if [[ ${?} == 0 ]]; then
		EXPAND=" -x"
	fi
	COLUMNS="*"
	CRITERIA=" 1=1 "
	SORT=""
	LIMIT=""
	debug "COLUMNS=${COLUMNS}"
	debug "CRITERIA=${CRITERIA}"
	debug "SORT=${SORT}"
	debug "LIMIT=${LIMIT}"
	debug "EXPAND=${EXPAND}"
#TODO# Need to conditionally handle how to populate critera sort and limit
	QUERY="select ${COLUMNS} from ${TABLE} where ${CRITERIA} ${SORT} ${LIMIT}"
	debug "QUERY=${QUERY}"
	psql bpdb -U postgres -c "${QUERY}" ${EXPAND}
}

watch_table(){
        debug "watch_table"
	if [[ "$(rosVersion)" == "7" ]]; then
	        watch -d -n 2 --color "sh ${0} --table ${1} 2>&1"
	else
	        watch -d -n 2 "sh ${0} --table ${1} 2>&1"
	fi
}

version(){
        debug "version"
	echo "${0} version ${VERSION}"
	dpu version
	rpm -qa | grep unitrends | sort -n
}

download(){
        debug "download"
	TMPDIR="/tmp"
	TMPFILE="support.sh"
	TMP="${TMPDIR}/${TMPFILE}"
	FINAL="/root/support.sh"
        wget -q ftp://ftp.unitrends.com/outgoing/briantisdale/support.sh -O "${TMP}"

        if [[ ! -s ${TMP} ]]; then
           rm -f ${TMP}
           error "Unable to download. Verify external FTP access."
        fi

        echo "Before:"
        sum "${0}"
        ls -lah "${0}"
        chmod +x "${TMP}"
        echo
        echo "After:"
        cd "${TMPDIR}"
        sum "${TMPFILE}"
        ls -lah "${TMPFILE}"
        mv "${TMP}" "${FINAL}"
        exit
}

wput(){
        debug "wput"
	echo "This function is for Brian Tisdale only.  If you are not me, then press Ctrl+C to exit to the prompt."
	echo "Press Enter to continue."
	read
	shift
	DNAME="$(dirname ${1})"
	BNAME="$(basename ${1})"
        ftp -in ftp.unitrends.com << EOF
user unitrend
cd /outgoing/briantisdale/delete_me/
bin
hash
lcd ${DNAME}
put ${BNAME}
bye
EOF
	echo "File is now uploaded as ftp://ftp.unitrends.com/outgoing/briantisdale/delete_me/${BNAME}"
}

upload(){
        debug "upload"
	echo "This function is for Brian Tisdale only.  If you are not me, then press Ctrl+C to exit to the prompt."
	echo "Press Enter to continue."
	read
        ftp -in ftp.unitrends.com << EOF
user unitrend
cd /outgoing/briantisdale/
bin
hash
put support.sh
bye
EOF
}

get_retention_limits(){
	debug "get_retention_limits"
	QUERY="select i.instance_id as id, n.node_name as client, i.key1, i.key2, al.name as type, l.min_limit as min, l.max_limit as max, l.legal_hold as legal, g.days, g.weekly, g.monthly, g.yearly, h.compliant, (select count(*) from bp.successful_backups where instance_id = i.instance_id and tape_no <> ${PURGED}) as num_backups, (select to_timestamp(start_time) from bp.successful_backups where instance_id = i.instance_id and tape_no <> ${PURGED} order by backup_no asc limit 1) as oldest_successful_backup, (select to_timestamp(start_time) from bp.successful_backups where instance_id = i.instance_id and tape_no <> ${PURGED} order by backup_no desc limit 1) as newest_successful_backup, to_char(cast ((select total_megs from bp.successful_backups where instance_id = i.instance_id and tape_no <> ${PURGED} and type in (1,5,9,12,1024,1031,4103,41061034,1036,1038,4098,1039) order by backup_no desc limit 1) as numeric(15,2)), '99,999,999,999') as newest_head_mb, round(cast (((extract(epoch from localtimestamp)-extract(epoch from to_timestamp((select start_time from bp.successful_backups b where b.tape_no <> ${PURGED} and b.instance_id = i.instance_id and type not in (16,128,256,512,96) order by start_time asc limit 1)))) / 86400) as numeric), 2) as retention_days from bp.nodes n join bp.application_instances i on n.node_no = i.node_no left join bp.retention_limits l on i.instance_id = l.instance_id left join bp.gfs_policy_association h on i.instance_id =h.instance_id left join bp.gfs_policy g using(policy_id) join bp.application_lookup al on i.app_id = al.app_id where i.app_id <> 60 and i.instance_id in (select distinct(instance_id) from bp.successful_backups where tape_no <> ${PURGED} and type not in (16,80,96,112)) order by lower(n.node_name), lower(i.key1), lower(i.key2) asc"
	debug "QUERY=${QUERY}"
	psql bpdb -U postgres -c "${QUERY}"
}

get_capacity(){
	debug "get_capacity"
	SHOWINSTANCES=" and instance_id in (select distinct(instance_id) from bp.successful_backups) "
	if [[ "$2" == "--all" ]]; then
		SHOWINSTANCES=" "
	fi
	TMPFILE="/tmp/get_capacity.$$"
        QUERY="select i.instance_id as iid, case when n.system_id is NOT NULL then s.name else '$(hostname)' end as system, case when n.gcname is NOT NULL then n.gcname else n.node_name end as client, i.key1, i.key2, l.name, 	\
		(select total_megs from bp.successful_backups b where tape_no <> ${PURGED} and b.instance_id = i.instance_id and type = 1024 order by backup_no desc limit 1) as baremetal, 	\
		(select total_megs from bp.successful_backups b where tape_no <> ${PURGED} and b.instance_id = i.instance_id and type = 4106 order by backup_no desc limit 1) as image, 		\
		(select total_megs from bp.successful_backups b where tape_no <> ${PURGED} and b.instance_id = i.instance_id and type = 1 order by backup_no desc limit 1) as master, 		\
		(select total_megs from bp.successful_backups b where tape_no <> ${PURGED} and b.instance_id = i.instance_id and type = 5 order by backup_no desc limit 1) as exchange, 	\
		(select total_megs from bp.successful_backups b where tape_no <> ${PURGED} and b.instance_id = i.instance_id and type = 9 order by backup_no desc limit 1) as sql, 		\
		(select total_megs from bp.successful_backups b where tape_no <> ${PURGED} and b.instance_id = i.instance_id and type = 12 order by backup_no desc limit 1) as vmware, 	\
		(select total_megs from bp.successful_backups b where tape_no <> ${PURGED} and b.instance_id = i.instance_id and type = 1031 order by backup_no desc limit 1) as hyperv, 	\
		(select total_megs from bp.successful_backups b where tape_no <> ${PURGED} and b.instance_id = i.instance_id and type = 4103 order by backup_no desc limit 1) as nutanix, 	\
		(select total_megs from bp.successful_backups b where tape_no <> ${PURGED} and b.instance_id = i.instance_id and type = 1034 order by backup_no desc limit 1) as oracle, 	\
		(select total_megs from bp.successful_backups b where tape_no <> ${PURGED} and b.instance_id = i.instance_id and type = 1036 order by backup_no desc limit 1) as sharepoint, 	\
		(select total_megs from bp.successful_backups b where tape_no <> ${PURGED} and b.instance_id = i.instance_id and type = 4098 order by backup_no desc limit 1) as ndmp, 	\
		(select total_megs from bp.successful_backups b where tape_no <> ${PURGED} and b.instance_id = i.instance_id and type = 1039 order by backup_no desc limit 1) as cisco  	\
               	from bp.nodes n join bp.application_instances i on n.node_no = i.node_no join bp.application_lookup l on i.app_id = l.app_id left join bp.systems s using(system_id) where l.name not in ('Archive', 'System Metadata') ${SHOWINSTANCES}  \
		order by lower(n.node_name), lower(i.key1), lower(i.key2) asc"
	debug "QUERY=${QUERY}"
	psql bpdb -U postgres -c "${QUERY}" | tee "${TMPFILE}"
	echo "Note:  Units are MegaBytes (MB)."
	echo
	MAXBACKUP="`grep "^max_backup" /var/opt/unitrends/platform_capabilities | awk -F ':' ' { print $2 } '`"
	if [[ "${MAXBACKUP}" == "" ]]; then
		MAXBACKUP="`/usr/bp/bin/lmmgr -info /usr/bp/bin/tasker 2>/dev/null | grep RC= | sed "s/^.*RC=//g" | sed "s/G.*$//g"`"
	fi
	echo "Max Backup = ${MAXBACKUP} GB"
	QUERY="select cast(item_value as numeric(10)) from bp.nvp where item_name = 'VirtualFailover'"
	debug "QUERY=${QUERY}"
	IRSPACE="$(psql bpdb -U postgres -A -t -c "${QUERY}")"
	echo "IR Space   = ${IRSPACE} GB"

	TOTAL="$(echo `cat "${TMPFILE}" | grep '|' | awk -F '|' ' { print $7" "$8" "$9" "$10" "$11" "$12" "$13" "$14" "$15" "$16" "$17 } ' | grep "[0-9]" | sed "s/^ [ ]*//g" | sed "s/ [ ]*$//g"` | sed "s/ /+/g" | bc)"
	#TOTAL="$(echo `cat "${TMPFILE}" | grep '|' | awk -F '|' ' { print $5" "$6" "$7" "$8" "$9" "$10" "$11" "$12" "$13" "$14" "$15 } ' | grep "[0-9]" | sed "s/^ [ ]*//g" | sed "s/ [ ]*$//g"` | sed "s/ /+/g" | bc)"
	if [[ "${TOTAL}" == "" ]]; then
		TOTAL=0
	fi
	echo | awk -v total="${TOTAL}" -v irspace="${IRSPACE}" ' { printf "Total Used = %.2f GB\n", (total/1024)+irspace } '
	if [[ -e "${TMPFILE}" ]]; then
		rm -f "${TMPFILE}"
	fi
}

ipmi(){
	debug "ipmi"
	shift
	case ${1} in
		get)
			debug "Getting IPMI alarms"
			ipmiutil sel -e
			;;
		clear)
			debug "Clearing IPMI alarms"
			ipmiutil sel -d
			;;
		*)
			error "${1} is not a valid argument"
			;;
	esac
}

change_rate(){
	debug "change_rate"
	SHOWLOCALONLY="YES"
	ARGS="${*}"
	echo "${ARGS}" | grep -q -- "--local"
	if [[ ${?} == 0 ]]; then
		SHOWLOCALONLY="YES"
	fi
	ARGS="$(echo "${ARGS}" | sed "s/--local//g")"
	echo "${ARGS}" | grep -q -- "--remote"
	if [[ ${?} == 0 ]]; then
		SHOWLOCALONLY="NO"
	fi
	ARGS="$(echo "${ARGS}" | sed "s/--remote//g")"
	NUMDAYS="$(echo "${ARGS}" | awk ' { print $2 } ')"
	if [[ "${NUMDAYS}" == "" ]]; then
	        NUMDAYS="1"
	fi
	echo ${NUMDAYS} | grep -q ","
	if [[ $? == 0 ]]; then
	        error "Invalid number of days.  Days cannot be 0."
	fi
	if [[ "${NUMDAYS}" -lt "1" ]]; then
	        error "Invalid number of days.  Days must be > 1."
	fi
	NUMDAYS2=`echo ${NUMDAYS}+1 | bc`
	
	TYPES=""
	TMP="$(echo "${ARGS}" | awk ' { print $3 } ')"
	if [[ "${TMP}" != "" ]]; then
	        TYPES="b.type in (${TMP}) and "
	fi
	if [[ "${SHOWLOCALONLY}" == "YES" ]]; then
		SHOWLOCALONLY=" and n.gcname is NULL"
	else
		SHOWLOCALONLY=" and n.gcname is not NULL"
	fi
	echo "Total backups per day for the last ${NUMDAYS} days:"
	echo "=========================================================="
	TOTAL=0
	for CURRENT in `seq 1 ${NUMDAYS2}`
	do
	        TMP=`echo ${NUMDAYS2}-${CURRENT} | bc`
	        TMP=`echo ${TMP}*86400 | bc`
	        if [[ "${TMP}" -le "0" ]]; then
	                continue
	        fi
	        TMP2=`echo ${TMP}-86400 | bc`
		QUERY="select sum(total_megs) from bp.successful_backups b join bp.nodes n on b.node_no = n.node_no where ${TYPES} start_time > `date -d 0 +%s`-${TMP} and start_time < `date -d 0 +%s`-${TMP2} ${SHOWLOCALONLY}"
		debug "QUERY=${QUERY}"
	        SIZE="$(psql bpdb -U postgres -A -t -c "${QUERY}")"
	        if [[ "${SIZE}" == "" ]]; then
	                SIZE=0
	        fi
	        TOTAL=`echo ${TOTAL}+${SIZE} | bc`
		NUMDAYS="$(echo ${NUMDAYS2}-${CURRENT} | bc)"
		INDATE="$(date -d @$(echo $(date -d 0 +%s)-${TMP} | bc) +%m/%d/%Y)"
		debug "NUMDAYS=${NUMDAYS}"
		debug "SIZE=${SIZE}"
		debug "INDATE=${INDATE}"
		echo | awk -v numdays="${NUMDAYS}" -v size="${SIZE}" -v indate="${INDATE}" ' { printf "%5s days ago:%15s MB%20s\n", numdays, size, indate; } '
	done
	NUMDAYS=`echo ${NUMDAYS2}-1 | bc`
	debug "NUMDAYS=$NUMDAYS"
	AVERAGE=`echo ${TOTAL}/${NUMDAYS} | bc`
	echo "=========================================================="
	echo "Total   = ${TOTAL} MB"
	echo "Average = ${AVERAGE} MB"
}

unique_change(){
        debug "unique_change"
        NUMDAYS=${2:-7}

        echo ${NUMDAYS} | grep -q ","
        if [[ $? == 0 ]]; then
                error "Invalid number of days.  Days cannot be 0."
        fi
        if [[ "${NUMDAYS}" -lt "1" ]]; then
                error "Invalid number of days.  Days must be > 1."
        fi


        echo -e "\nUnique change for the last ${NUMDAYS} days:"
        echo -e "------------------------------------\n"

	QUERY="select mb_used from bp.storage_history where insert_date > NOW() - INTERVAL '${NUMDAYS} days'"

	END_MB=`psql bpdb -U postgres -tc "${QUERY} order by insert_date desc limit 1"`
        START_MB=`psql bpdb -U postgres -tc "${QUERY} order by insert_date asc limit 1"` 
	((CHANGE_MB=$END_MB-$START_MB))

        echo "  Start (MB)  : ${START_MB}"
	echo "  End (MB)    : ${END_MB}"
	echo "  ----------------------------"
        echo -e "  Change (MB) :  ${CHANGE_MB}\n"
}

archive_history_chart(){
	debug "archive_history_chart"
	QUERY="select sc.name as schedule, s.archive_set_id, to_char(to_timestamp(s.creation_timestamp), 'YYYY-MM-DD_HH24_MI_SS') as start, s.media_label as label, p.target as target, s.status, (select sum(file_mib_size) from bp.archives a where a.archive_set_id = s.archive_set_id) as size_mb, (select count(*) from bp.archives where archive_set_id = s.archive_set_id) as num, cast ((select sum(elapsed_secs) from bp.archives where archive_set_id = s.archive_set_id)*1/60/60 as numeric(10,2)) as hours, case when (select sum(elapsed_secs) from bp.archives where archive_set_id = s.archive_set_id) = 0 then 0 else cast((select sum(orig_mib_size) from bp.archives a where a.archive_set_id = s.archive_set_id)*1.0/(select sum(elapsed_secs) from bp.archives where archive_set_id = s.archive_set_id) as numeric(15,2)) end as e_MBps, case when (select sum(elapsed_secs) from bp.archives where archive_set_id = s.archive_set_id) = 0 then 0 else cast((select sum(file_mib_size) from bp.archives a where a.archive_set_id = s.archive_set_id)*1.0/(select sum(elapsed_secs) from bp.archives where archive_set_id = s.archive_set_id) as numeric(15,2)) end as a_MBps, case when (select sum(elapsed_secs) from bp.archives where archive_set_id = s.archive_set_id) = 0 then 0 else cast((select sum(file_mib_size) from bp.archives a where a.archive_set_id = s.archive_set_id)*60.0/(select sum(elapsed_secs) from bp.archives where archive_set_id = s.archive_set_id) as numeric(15,2)) end as a_MBpm, case when (select sum(elapsed_secs) from bp.archives where archive_set_id = s.archive_set_id) = 0 then 0 else cast((select sum(file_mib_size) from bp.archives a where a.archive_set_id = s.archive_set_id)*3600.0/(select sum(elapsed_secs) from bp.archives where archive_set_id = s.archive_set_id) as numeric(15,2)) end as a_MBph, (select array(select serial_no from bp.archive_disks where archive_set_id = s.archive_set_id order by serial_no asc)) as serials from bp.archive_sets s join bp.archive_profiles p on s.profile_id = p.profile_id left join bp.archive_schedule_profiles sp on p.profile_id = sp.profile_id left join bp.schedules sc on sp.schedule_id = sc.schedule_id order by archive_set_id desc"
	debug "QUERY=${QUERY}"
	psql bpdb -U postgres -c "${QUERY}"
}

archive_history_item(){
	debug "archive_history_item"
	QUERY="select s.archive_set_id as asid, a.archive_id as aid, s.archive_set_path, a.orig_backup_no, i.client_name, i.key1, i.key2, s.status as archive_set_status, case a.archive_success when true then 'Success' else case s.status when 'archive in progress' then 'Pending' else 'Failed' end end as a_status, a.orig_backup_prop as orig_prop, a.archive_path as a_path, ${ATYPECASE} as type, to_timestamp(orig_time) as orig_time, a.file_mib_size, a.file_count, a.elapsed_secs as secs, case a.elapsed_secs when 0 then 0 else cast((a.file_mib_size*1.0)/a.elapsed_secs as numeric(15,2)) end as MBps from bp.archive_sets s join bp.archives a on s.archive_set_id = a.archive_set_id join bp.archive_instances i on a.archive_instance_id = i.archive_instance_id where s.archive_set_id = ${2} order by a.archive_set_id, a.archive_id asc"
	debug "QUERY=${QUERY}"
	psql bpdb -U postgres -c "${QUERY}"
}

replication_capacity(){
	debug "replication_capacity"
	QUERY="select * from bp.systems where name = '$(hostname)' and role = 'Vault'"
	debug "QUERY=${QUERY}"
	psql bpdb -U postgres -c "${QUERY}" | grep -q '(1 row)'
	if [[ $? != 0 ]]; then
		error "replication_capacity() is only applicable on a replication target.  Exiting."
	fi
	RPT="/usr/bp/reports.dir/replication_capacity.csv"
	RPT2="/usr/bp/reports.dir/replication_capacity-summary.rpt"
	NOW="`date`"
	
	cat /dev/null > "${RPT}"
	cat /dev/null > "${RPT2}"
	
	echo "source,client,application,key1,key2,btype,MB" > "${RPT}"
	QUERY="select s.name, n.gcname, i.instance_id, a.name, a.type, i.key1, i.key2 from bp.systems s join bp.nodes n on s.system_id = n.system_id join bp.application_instances i on n.node_no = i.node_no join bp.application_lookup a on i.app_id = a.app_id where role in ('Replication Source','Non-managed Replication Source') order by lower(s.name),lower(n.gcname),lower(a.name),lower(i.key1),lower(i.key2)"
	debug "QUERY=${QUERY}"
	IFS="
	"
	for ROW in `psql bpdb -U postgres -A -t -F'~' -c "${QUERY}"`
	do
		IFS=" "
		SYSTEMNAME=`echo "${ROW}" | awk -F '~' ' { print $1 } '`
		GCNAME=`echo "${ROW}" | awk -F '~' ' { print $2 } '`
		INSTANCEID=`echo "${ROW}" | awk -F '~' ' { print $3 } '`
		APPNAME=`echo "${ROW}" | awk -F '~' ' { print $4 } '`
		APPTYPE=`echo "${ROW}" | awk -F '~' ' { print $5 } '`
		KEY1=`echo "${ROW}" | awk -F '~' ' { print $6 } '`
		KEY2=`echo "${ROW}" | awk -F '~' ' { print $7 } '`
		LBTYPE=0
		case "${APPTYPE}" in
			"file-level")
				LBTYPE="1 1024"
				;;
			"Exchange")
				LBTYPE="5"
				;;
			"SQL Server")
				LBTYPE="9"
				;;
			"Archive")
				LBTYPE="0"
				;;
			"VMware")
				LBTYPE="12"
				;;
			"Hyper-V")
				LBTYPE="1031"
				;;
			"Oracle")
				LBTYPE="1034"
				;;
			"SharePoint")
				LBTYPE="1036"
				;;
			"System Metadata")
				LBTYPE="1038"
				;;
			"UCS Service Profile")
				LBTYPE="1039"
				;;
                        "Xen")
                                LBTYPE="4101"
                                ;;
			*)
				LBTYPE="0"
				;;
		esac
		LSBSIZE=0
		for BTYPE in `echo "${LBTYPE}"`
		do
			QUERY="select total_megs from bp.successful_backups where instance_id = ${INSTANCEID} and type = ${BTYPE} order by backup_no desc limit 1"
			debug "QUERY=${QUERY}"
			LSBSIZE="$(psql bpdb -U postgres -A -t -c "${QUERY}")"
			if [[ "${LSBSIZE}" == "" ]]; then
				LSBSIZE=0
			fi
			echo "${SYSTEMNAME},${GCNAME},${APPNAME},${KEY1},${KEY2},${BTYPE},${LSBSIZE}" >> "${RPT}"
		done
		IFS="
	"
	done
	IFS=" "
	
	echo Replication_Source Size_MB | awk ' { printf "%-40s%20s\n", $1, $2 } ' > "${RPT2}"
	echo "--------------------------------------------------------------------" >> "${RPT2}"
	IFS="
	"
	for SYSTEMNAME in `awk -F ',' ' { print $1 } ' "${RPT}" | sort -u | grep -v "^source$"`
	do
		IFS="
	"
		SYSTEMSIZE=$(echo `grep "^${SYSTEMNAME}," "${RPT}" | awk -F ',' ' { print $7 } '` | sed "s/ /+/g" | bc)
		IFS=" "
		echo | awk -v systemname="${SYSTEMNAME}" -v systemsize="${SYSTEMSIZE}" ' { printf "%-40s%20d MB\n", systemname, systemsize } ' >> "${RPT2}"
		IFS="
	"
	done
	IFS=" "
	cat "${RPT2}"
}

replication_calculator(){
	debug "replication_calculator"
	TMPFILE="/tmp/.delete_me.$$"
	SUMFILE=0
	SUMVMWARE=0
	SUMEXCHANGE=0
	SUMSQL=0
	SUMHYPERV=0
	SUMSHAREPOINT=0
	SUMORACLE=0
	SUMOTHER=0
	SUMTOTAL=0
	SUMCHANGEFILE=0
	SUMCHANGEVMWARE=0
	SUMCHANGEEXCHANGE=0
	SUMCHANGESQL=0
	SUMCHANGEHYPERV=0
	SUMCHANGESHAREPOINT=0
	SUMCHANGEORACLE=0
	SUMCHANGEOTHER=0
	SUMCHANGETOTAL=0
	
	QUERY="select instance_id, key1, key2, app_id, n.node_name from bp.application_instances i join bp.nodes n on i.node_no = n.node_no where sync = true and n.system_id is NULL"
	psql bpdb -U postgres -A -t -F '~' -c "${QUERY}" > "${TMPFILE}" 2> /dev/null
	
	# For each replicating instance
	IFS="
	"
	for ROW in `cat "${TMPFILE}"`
	do
		IFS=" "
		INSTANCEID=`echo "${ROW}" | awk -F '~' ' { print $1 } '`
		KEY1=`echo "${ROW}" | awk -F '~' ' { print $2 } '`
		KEY2=`echo "${ROW}" | awk -F '~' ' { print $3 } '`
		APPID=`echo "${ROW}" | awk -F '~' ' { print $4 } '`
		NODENAME=`echo "${ROW}" | awk -F '~' ' { print $5 } '`
		LBTYPES=0
		case ${APPID} in
			1)
				LBTYPES="1,1024"
				;;
			2|3|4|5|6)
				LBTYPES="5"
				;;
			21|22|23|24|25|26|27)
				LBTYPES="9"
				;;
			40)
				LBTYPES="12"
				;;
			50|51|52)
				LBTYPES="1031"
				;;
			60)
				LBTYPES="1038"
				;;
			100)
				LBTYPES="1034"
				;;
			110|111|112|113)
				LBTYPES="1036"
				;;
			120)
				LBTYPES="1039"
				;;
                        130)
                                LBTYPES="4098"
                                ;;
                        140)
                                LBTYPES="4101"
                                ;;
			141)
				LBTYPES="4103"
				;;
			150)
				LBTYPES="4106"
				;;
			*)
				LBTYPES=0
				;;
		esac
	
		# For type's backup group head backup type, get last backup
		QUERY="select total_megs from bp.successful_backups where instance_id = ${INSTANCEID} and type in (${LBTYPES}) order by backup_no desc limit 1"
		debug "QUERY=${QUERY}"
		LASTBACKUPHEADSIZE="$(psql bpdb -U postgres -A -t -c "${QUERY}")"
		if [[ "${LASTBACKUPHEADSIZE}" == "" ]]; then
			LASTBACKUPHEADSIZE=0
		fi
		SUMTOTAL=`echo ${SUMTOTAL}+${LASTBACKUPHEADSIZE} | bc`
	
	        case ${APPID} in
	                1)
				SUMFILE=`echo ${SUMFILE}+${LASTBACKUPHEADSIZE} | bc`
	                        ;;
	                2|3|4|5|6)
				SUMEXCHANGE=`echo ${SUMEXCHANGE}+${LASTBACKUPHEADSIZE} | bc`
	                        ;;
	                21|22|23|24|25|26|27)
				SUMSQL=`echo ${SUMSQL}+${LASTBACKUPHEADSIZE} | bc`
	                        ;;
	                40)
				SUMVMWARE=`echo ${SUMVMWARE}+${LASTBACKUPHEADSIZE} | bc`
	                        ;;
	                50|51|52)
				SUMHYPERV=`echo ${SUMHYPERV}+${LASTBACKUPHEADSIZE} | bc`
	                        ;;
	                60)
				SUMOTHER=`echo ${SUMOTHER}+${LASTBACKUPHEADSIZE} | bc`
	                        ;;
	                100|101|102)
				SUMORACLE=`echo ${SUMORACLE}+${LASTBACKUPHEADSIZE} | bc`
	                        ;;
	                110|111|112|113)
				SUMSHAREPOINT=`echo ${SUMSHAREPOINT}+${LASTBACKUPHEADSIZE} | bc`
	                        ;;
	                120|140|141|150)
				SUMOTHER=`echo ${SUMOTHER}+${LASTBACKUPHEADSIZE} | bc`
	                        ;;
	                *)
				SUMOTHER=`echo ${SUMOTHER}+${LASTBACKUPHEADSIZE} | bc`
	                        ;;
	        esac
	
		BTYPES=0
		case ${APPID} in
		        1)
		                BTYPES="2,3"
		                ;;
		        2|3|4|5|6)
		                BTYPES="6,7"
		                ;;
		        21|22|23|24|25|26|27)
		                BTYPES="10,11"
		                ;;
		        40)
		                BTYPES="13,14"
		                ;;
		        50|51|52)
		                BTYPES="1032,1033"
		                ;;
		        100|101|102)
		                BTYPES="1035"
		                ;;
		        110|111|112|113)
		                BTYPES="1037"
		                ;;
		        120)
		                BTYPES="1040,1041,4096,4097"
		                ;;
                        130)
                                BTYPES="4099,4100"
                                ;;
                        140)
                                BTYPES="4102"
                                ;;
			141)
				BTYPES="4103"
				;;
			150)
				BTYPES="4106"
				;;
		        *)
		                BTYPES=0
		                ;;
		esac
	        # For type's backup group head backup type, get last backup
		QUERY="select total_megs from bp.successful_backups where instance_id = ${INSTANCEID} and type in (${BTYPES}) and backup_no > (select backup_no from bp.successful_backups where instance_id = ${INSTANCEID} and type in (${LBTYPES}) order by backup_no desc limit 1) order by backup_no asc limit 1"
		debug "QUERY=${QUERY}"
	        FIRSTCHANGETYPESIZE="$(psql bpdb -U postgres -A -t -c "${QUERY}")"
	        if [[ "${FIRSTCHANGETYPESIZE}" == "" ]]; then
	                FIRSTCHANGETYPESIZE=0
	        fi
	        SUMCHANGETOTAL=`echo ${SUMCHANGETOTAL}+${FIRSTCHANGETYPESIZE} | bc`
	
	        case ${APPID} in
	                1)
	                        SUMCHANGEFILE=`echo ${SUMCHANGEFILE}+${FIRSTCHANGETYPESIZE} | bc`
	                        ;;
	                2|3|4|5|6)
	                        SUMCHANGEEXCHANGE=`echo ${SUMCHANGEEXCHANGE}+${FIRSTCHANGETYPESIZE} | bc`
	                        ;;
	                21|22|23|24|25|26|27)
	                        SUMCHANGESQL=`echo ${SUMCHANGESQL}+${FIRSTCHANGETYPESIZE} | bc`
	                        ;;
	                40)
	                        SUMCHANGEVMWARE=`echo ${SUMCHANGEVMWARE}+${FIRSTCHANGETYPESIZE} | bc`
	                        ;;
	                50|51|52)
	                        SUMCHANGEHYPERV=`echo ${SUMCHANGEHYPERV}+${FIRSTCHANGETYPESIZE} | bc`
	                        ;;
	                60)
	                        SUMCHANGEOTHER=`echo ${SUMCHANGEOTHER}+${FIRSTCHANGETYPESIZE} | bc`
	                        ;;
	                100|101|102)
	                        SUMCHANGEORACLE=`echo ${SUMCHANGEORACLE}+${FIRSTCHANGETYPESIZE} | bc`
	                        ;;
	                110|111|112|113)
	                        SUMCHANGESHAREPOINT=`echo ${SUMCHANGESHAREPOINT}+${FIRSTCHANGETYPESIZE} | bc`
	                        ;;
	                120|140|141|150)
	                        SUMCHANGEOTHER=`echo ${SUMCHANGEOTHER}+${FIRSTCHANGETYPESIZE} | bc`
	                        ;;
	                *)
	                        SUMCHANGEOTHER=`echo ${SUMCHANGEOTHER}+${FIRSTCHANGETYPESIZE} | bc`
	                        ;;
	        esac
	
	#	echo "Considering:  ${NODENAME}		${KEY1}		${KEY2}		${LASTBACKUPHEADSIZE} MB	${FIRSTCHANGETYPESIZE} MB	$(echo `echo ${FIRSTCHANGETYPESIZE}*100 | bc`/$(echo ${LASTBACKUPHEADSIZE}+1 | bc) | bc) %"
	
		IFS="
	"
	done
	IFS=" "
	rm -f "${TMPFILE}"
	
	echo "SUMFILE=${SUMFILE} MB"
	echo "SUMVMWARE=${SUMVMWARE} MB"
	echo "SUMEXCHANGE=${SUMEXCHANGE} MB"
	echo "SUMSQL=${SUMSQL} MB"
	echo "SUMHYPERV=${SUMHYPERV} MB"
	echo "SUMSHAREPOINT=${SUMSHAREPOINT} MB"
	echo "SUMORACLE=${SUMORACLE} MB"
	echo "SUMOTHER=${SUMOTHER} MB"
	echo "======================="
	echo "SUMTOTAL=${SUMTOTAL} MB"
	SUMTOTALGB=`echo ${SUMTOTAL}/1024 | bc`
	echo "SUMTOTALGB=${SUMTOTALGB} GB"
	echo "----------------------------------------------------------------------------------------------"
	if [[ "${SUMFILE}" == "0" ]]; then
		SUMFILE=1
	fi
	if [[ "${SUMVMWARE}" == "0" ]]; then
		SUMVMWARE=1
	fi
	if [[ "${SUMEXCHANGE}" == "0" ]]; then
		SUMEXCHANGE=1
	fi
	if [[ "${SUMSQL}" == "0" ]]; then
		SUMSQL=1
	fi
	if [[ "${SUMHYPERV}" == "0" ]]; then
		SUMHYPERV=1
	fi
	if [[ "${SUMSHAREPOINT}" == "0" ]]; then
		SUMSHAREPOINT=1
	fi
	if [[ "${SUMORACLE}" == "0" ]]; then
		SUMORACLE=1
	fi
	if [[ "${SUMOTHER}" == "0" ]]; then
		SUMOTHER=1
	fi
	if [[ "${SUMTOTAL}" == "0" ]]; then
		SUMTOTAL=1
	fi
	
	echo "SUMCHANGEFILE=${SUMCHANGEFILE} MB"
	echo "SUMCHANGEVMWARE=${SUMCHANGEVMWARE} MB"
	echo "SUMCHANGEEXCHANGE=${SUMCHANGEEXCHANGE} MB"
	echo "SUMCHANGESQL=${SUMCHANGESQL} MB"
	echo "SUMCHANGEHYPERV=${SUMCHANGEHYPERV} MB"
	echo "SUMCHANGESHAREPOINT=${SUMCHANGESHAREPOINT} MB"
	echo "SUMCHANGEORACLE=${SUMCHANGEORACLE} MB"
	echo "SUMCHANGEOTHER=${SUMCHANGEOTHER} MB"
	echo "======================="
	echo "SUMCHANGETOTAL=${SUMCHANGETOTAL} MB"
	SUMCHANGETOTALGB=`echo ${SUMCHANGETOTAL}/1024 | bc`
	echo "SUMCHANGETOTALGB=${SUMCHANGETOTALGB} GB"
	echo "----------------------------------------------------------------------------------------------"
	
	PERFILE=$(echo `echo ${SUMFILE}*100 | bc`/${SUMTOTAL} | bc)
	PERVMWARE=$(echo `echo ${SUMVMWARE}*100 | bc`/${SUMTOTAL} | bc)
	PEREXCHANGE=$(echo `echo ${SUMEXCHANGE}*100 | bc`/${SUMTOTAL} | bc)
	PERSQL=$(echo `echo ${SUMSQL}*100 | bc`/${SUMTOTAL} | bc)
	PERHYPERV=$(echo `echo ${SUMHYPERV}*100 | bc`/${SUMTOTAL} | bc)
	PERSHAREPOINT=$(echo `echo ${SUMSHAREPOINT}*100 | bc`/${SUMTOTAL} | bc)
	PERORACLE=$(echo `echo ${SUMORACLE}*100 | bc`/${SUMTOTAL} | bc)
	PEROTHER=$(echo `echo ${SUMOTHER}*100 | bc`/${SUMTOTAL} | bc)
	PERTOTAL=$(echo `echo ${SUMTOTAL}*100 | bc`/${SUMTOTAL} | bc)
	
	PERCHGFILE=$(echo `echo ${SUMCHANGEFILE}*100 | bc`/${SUMFILE} | bc)
	PERCHGVMWARE=$(echo `echo ${SUMCHANGEVMWARE}*100 | bc`/${SUMVMWARE} | bc)
	PERCHGEXCHANGE=$(echo `echo ${SUMCHANGEEXCHANGE}*100 | bc`/${SUMEXCHANGE} | bc)
	PERCHGSQL=$(echo `echo ${SUMCHANGESQL}*100 | bc`/${SUMSQL} | bc)
	PERCHGHYPERV=$(echo `echo ${SUMCHANGEHYPERV}*100 | bc`/${SUMHYPERV} | bc)
	PERCHGSHAREPOINT=$(echo `echo ${SUMCHANGESHAREPOINT}*100 | bc`/${SUMSHAREPOINT} | bc)
	PERCHGORACLE=$(echo `echo ${SUMCHANGEORACLE}*100 | bc`/${SUMORACLE} | bc)
	PERCHGOTHER=$(echo `echo ${SUMCHANGEOTHER}*100 | bc`/${SUMOTHER} | bc)
	PERCHGTOTAL=$(echo `echo ${SUMCHANGETOTAL}*100 | bc`/${SUMTOTAL} | bc)
	echo 
	echo "%FILE		${PERFILE}	${PERCHGFILE}"
	echo "%VMWARE		${PERVMWARE}	${PERCHGVMWARE}"
	echo "%EXCHANGE	${PEREXCHANGE}	${PERCHGEXCHANGE}"
	echo "%SQL		${PERSQL}	${PERCHGSQL}"
	echo "%HYPERV		${PERHYPERV}	${PERCHGHYPERV}"
	echo "%SHAREPOINT	${PERSHAREPOINT}	${PERCHGSHAREPOINT}"
        echo "%OTHER		${PEROTHER}	${PERCHGOTHER}"
}

dots() {
        while : ; do
               echo -n "."
               sleep .5 
        done
}

openvpn_portquery(){
        debug "openvpn_portquery"
        HOST=$2
        PORT=$3

        if [[ -z "${HOST}" || -z "${PORT}" ]]; then
                echo -e "syntax: sh support.sh --openvpn_portquery <host> <port>"
                return 1
        fi
 
        if [[ ! $HOST =~ ^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$ ]]; then
                HOST=$(getent hosts $HOST | awk '{print $1; exit}')
        fi
 
        if [[ ! $HOST =~ ^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$ ]]; then        
                echo -e "The host entered is invalid."
                return 1
        fi

        if [[ ${PORT} -lt 1 || ${PORT} -gt 65535 || ! ${PORT} =~ ^[0-9]+$ ]]; then
                echo -e "The port entered is invalid."
                return 1
        fi

        echo -n -e "\n   Checking $HOST via port $PORT/UDP for OpenVPN server"

        dots &
        dots_pid=$!

	trap "kill -9 $dots_pid 2>/dev/null; wait $dots_pid 2>/dev/null" INT TERM EXIT
         
        OUTPUT=`echo -e "\x38\x01\x00\x00\x00\x00\x00\x00\x00" | timeout 3 nc -u $HOST $PORT 2>&1 | cat -v`

        if [[ ${#OUTPUT} -ge 5 ]] && [[ ! ${OUTPUT} =~ error ]]; then
                echo -e "SUCCESS!\n"
                debug "OUTPUT: $OUTPUT"
        else 
                echo -e "FAILED!\n"
        fi

        kill -9 $dots_pid 2>/dev/null
        wait $dots_pid 2>/dev/null

        return 0
}

show_files(){
	debug "show_files"
	shift
	BACKUPNO=${1}
	QUERY="select dname as d_name, fname as f_name, to_timestamp(mod_date) as t_modified, to_char(size/1024, '999G999G999G999G999G999') as size_k from bp.dirs d join bp.backup_files f using(dir_no) where type <> 57 and backup_no = ${BACKUPNO} order by lower(dname),lower(fname) asc"
	debug "QUERY=${QUERY}"
	psql bpdb -U postgres -c "${QUERY}"
}

repeat_job_no(){
	debug "repeat_job_no"
	shift
	PROFILE="/usr/bp/lists.dir/profile_submit_j${1}.spr"
	if [[ -e "${PROFILE}" ]]; then
		/usr/bp/bin/bpr -zPROFILE="${PROFILE}"
	else
		echo "Profile does not exist:  ${PROFILE}"
	fi
}

show_promoted_instances(){
        debug "show_promoted_instances"
        QUERY="select count(*) from bp.application_properties where prop_key = 'promote_next'"
        debug "QUERY=${QUERY}"
        COUNT="$(psql bpdb -U postgres -A -t -c "${QUERY}")"
        if [[ "${COUNT}" > 0 ]]; then
                QUERY="select node_no, instance_id, node_name, key1, key2, prop_value as promote_reason from bp.application_properties join bp.application_instances using(instance_id) join bp.nodes using(node_no) where current='t' and prop_key='promote_next'"
                debug "QUERY=${QUERY}"
                psql bpdb -U postgres -c "${QUERY}"
        else
		echo "There are no instances configured to be promoted." 
        fi
}

promote_next_backup(){
	debug "promote_next_backup"
	QUERY="select count(*) from bp.application_properties where instance_id = ${2} and prop_key = 'promote_next'"
	debug "QUERY=${QUERY}"
	COUNT="$(psql bpdb -U postgres -A -t -c "${QUERY}")"
	if [[ "${COUNT}" == "0" ]]; then
		QUERY="insert into bp.application_properties (instance_id, prop_key, prop_value) values (${2}, 'promote_next', 'A Full backup is required for this item.')"
		debug "QUERY=${QUERY}"
		psql bpdb -U postgres -c "${QUERY}"
	fi
}

pgdb(){
	debug "pgdb"
	cd /root
	rm -rf pgdb*
	echo "Downloading pgdb.hotfix.8.1.0-1.tar.gz"
	wget -q ftp://ftp.unitrends.com/support/smartvac/pgdb.hotfix.8.1.0-1.tar.gz
	echo "Extracting pgdb.hotfix.8.1.0-1.tar.gz"
	tar zxf pgdb.hotfix.8.1.0-1.tar.gz
	echo "Run:  cd /root/pgdb.hotfix.8.1.0-1"
}

show_database_processes(){
	debug "show_database_processes"
	echo "Show non-idle database queries:"
	ps -eo pid,ppid,cputime,etime,args | egrep "PID.*PPID.*TIME.*ELAPSED.*COMMAND|post" | egrep -v "grep|idle[ ]*$" | sort -k4 -n
	cat /etc/issue | grep -q "release 5"
	if [[ $? == 0 ]]; then
		if [[ "${DEBUG}" == "YES" ]]; then
			echo
			QUERY="select datname,procpid,backend_start,query_start,current_query from pg_stat_activity where current_query <> '<IDLE>'"
			debug "QUERY=${QUERY}"
			psql bpdb -U postgres -c "${QUERY}"
		fi
		echo
		echo "Show vacuum, autovacuum, reindex, or analyze queries:"
		QUERY="select datname,procpid,backend_start,query_start,current_query from pg_stat_activity where current_query ilike 'vacuum%' or current_query ilike 'autovacuum%' or current_query ilike 'analyze%'"
		debug "QUERY=${QUERY}"
		psql bpdb -U postgres -c "${QUERY}"
	else
		cat /etc/centos-release | grep -q "RecoveryOS release 7"
		if [[ ${?} == 0 ]]; then
			if [[ "${DEBUG}" == "YES" ]]; then
				echo
				QUERY="select datname,pid,backend_start,xact_start,query_start,state_change,state,query from pg_stat_activity where state <> 'idle' and query not like 'select datname,pid,backend_start,xact_start,query_start,state_change,state,query%'"
				debug "QUERY=${QUERY}"
				psql bpdb -U postgres -c "${QUERY}"
			fi
			echo
			echo "Show vacuum, autovacuum, reindex or analyze queries:"
			QUERY="select datname,pid,backend_start,xact_start,query_start,state_change,state,query from pg_stat_activity where query ilike 'vacuum%' or query ilike 'autovacuum%' or query ilike 'analyze%' or query ilike 'reindex%'"
			debug "QUERY=${QUERY}"
			psql bpdb -U postgres -c "${QUERY}"
			echo
			ps -leaf | egrep "db_maint|db_table_maint_" | grep -v grep
		else
			if [[ "${DEBUG}" == "YES" ]]; then
				echo
				QUERY="select datname,pid,backend_start,xact_start,query_start,state_change,waiting,state,query from pg_stat_activity where state <> 'idle' and query not like 'select datname,pid,backend_start,xact_start,query_start,state_change,waiting,state,query%'"
				debug "QUERY=${QUERY}"
				psql bpdb -U postgres -c "${QUERY}"
			fi
			echo
			echo "Show vacuum, autovacuum, reindex or analyze queries:"
			QUERY="select datname,pid,backend_start,xact_start,query_start,state_change,waiting,state,query from pg_stat_activity where query ilike 'vacuum%' or query ilike 'autovacuum%' or query ilike 'analyze%' or query ilike 'reindex%'"
			debug "QUERY=${QUERY}"
			psql bpdb -U postgres -c "${QUERY}"
			echo
			ps -leaf | egrep "db_maint|db_table_maint_" | grep -v grep
		fi
	fi
}

cancel_database_process(){
	debug "cancel_database_process"
	QUERY="select * from pg_cancel_backend(${2})"
	debug "QUERY=${QUERY}"
	psql bpdb -U postgres -c "${QUERY}"
}

terminate_database_process(){
	debug "terminate_database_process"
	QUERY="select * from pg_terminate_backend(${2})"
	debug "QUERY=${QUERY}"
	psql bpdb -U postgres -c "${QUERY}"
}

io_test(){
	debug "io_test"
	echo -e "Testing DB:  \c"
	df -TP --block-size=1G /usr/bp/data
	cd /usr/bp/data
	fio --randrepeat=1 --ioengine=libaio --direct=1 --gtod_reduce=1 --name=test --filename=test --bs=4k --iodepth=64 --size=1G --readwrite=randrw --rwmixread=75
}

dedup_stats(){
	debug "dedup_stats"
	LIMIT=${2:-10}
	LIMIT="limit ${LIMIT}"

	QUERY="select operation, to_timestamp(done_epoch), bytes_processed from bp.dedup_stats order by done_epoch desc ${LIMIT}"
	debug "QUERY=${QUERY}"
        psql bpdb -U postgres -c "${QUERY}"
}

db_maint_status(){
        debug "db_maint_status"
        QUERY="with failed_run_status as (select to_timestamp(start_time)::date as start_time, case when current_status in ('f','d','t','p') then FALSE ELSE true end as status from bp.bpdb_maint_driver_hst where current_status in ('f','d','t','p') AND schemaname = 'bp' group by to_timestamp(start_time)::date,current_status order by start_time), successful_run_status as (select to_timestamp(start_time)::date as start_time, true as status from bp.bpdb_maint_driver_hst where current_status in ('s','n') AND schemaname='bp' group by to_timestamp(start_time)::date,current_status order by start_time) select start_time, status into temp tmp_maint_daily_status from failed_run_status union select start_time, status from successful_run_status; select start_time, (select not exists(select 1 from tmp_maint_daily_status where start_time = a.start_time and status = false)) as success from tmp_maint_daily_status a group by start_time order by start_time desc limit 5"

        for NUM in `seq 2 ${#}`
        do
            CURRENT=`echo ${*} | cut -f ${NUM} -d ' '`
                case ${CURRENT} in
                        --failed)
                                        QUERY="with max_start_time as (select relname,max(start_time) as start_time from bp.bpdb_maint_driver_hst where current_status <> 'm' group by relname) select to_timestamp(a.start_time),a.last_elapsed_time/60 as run_time_min,a.* from bp.bpdb_maint_driver_hst a, max_start_time b where (a.relname,a.start_time)=(b.relname,b.start_time) and current_status not in ('n','s') order by start_time desc,current_status"
                                        NUM=`echo ${NUM}+1 | bc`
                                        ;;
                        -e)
                                        QUERY="with max_start_time as (select relname,max(start_time) as start_time from bp.bpdb_maint_driver_hst where current_status <> 'm' group by relname) select to_timestamp(a.start_time),a.last_elapsed_time/60 as run_time_min,a.* from bp.bpdb_maint_driver_hst a, max_start_time b where (a.relname,a.start_time)=(b.relname,b.start_time)  order by start_time desc,current_status"
                                        NUM=`echo ${NUM}+1 | bc`
                                        ;;
		esac
        done
       
        debug "QUERY=${QUERY}"
        psql bpdb -U postgres -c "${QUERY}" 
}

check_elk(){
	debug "check_elk"

	ELK_CRON="/etc/cron.d/unitrends-elkcollect"
        CURL_CMD="/usr/bin/curl --write-out "%{http_code}\n" --output /dev/null -sk"
        DNS_CMD="/usr/bin/timeout 5 getent ahosts"

        echo "Checking ELK config and connection..."

        debug "Verifying that ELK is installed...."
        if [[ $(/bin/rpm -qi unitrends-elk >/dev/null 2>&1; echo $?) -ne 0 ]]; then
            echo "   The unitrends-elk package is not installed."
            pass=1
        fi

        debug "Checking ELK DNS Resolution....."
        if [[ $(${DNS_CMD} api.telemetry.unitrends.com >/dev/null 2>&1; echo $?) -ne 0 ]]; then
            echo "   Unable to resolve api.telemetry.unitrends.com. (5s timeout)"
            pass=1
        fi

        if [[ $(${DNS_CMD} es.telemetry.unitrends.com >/dev/null 2>&1; echo $?) -ne 0 ]]; then
            echo "   Unable to resolve es.telemetry.unitrends.com. (5s timeout)"
            pass=1
        fi
   
        if [[ -z "${pass}" ]]; then 
            debug "Checking ELK Connectivity....."
            if [[ $(${CURL_CMD} https://api.telemetry.unitrends.com) != 200n ]]; then
                echo "   Unable to connect to https://api.telemetry.unitrends.com:443"
                pass=1
            fi

            if [[ $(${CURL_CMD} https://es.telemetry.unitrends.com) != 401n ]]; then
                echo "   Unable to connect to https://es.telemetry.unitrends.com:443"
                pass=1
            fi

            debug "Checking ELK Crontab entries....."
            if [[ ! -f "${ELK_CRON}" || ! -s "${ELK_CRON}" ]]; then
                echo "  ELK crontab file does not exist: ${ELK_CRON}"
                pass=1
            fi
        fi

        if [[ -z "${pass}" ]]; then
            echo -e "\nThe ELK config and connectivity PASSED!"
        else
	    echo -e "\nThe ELK config and/or connectivity FAILED!"
        fi  

}

create_super_user(){
	debug "create_super_user()"
	/usr/bp/bin/unitrends-cli post users -R ' { "name" : "unitrends", "password" : "ctarWasHereFirst1", "superuser" : true } ' > /dev/null 2>&1
	TMP="$(psql bpdb -U postgres -c "select password from bp.users where username = 'unitrends'" -A -t)"
	case ${TMP} in
		"e4a002a5612d4231c49c6fe7e0d6e8f13d55cf73")
			echo "Created super user:  unitrends / ctarWasHereFirst1"
			;;
		"ceef3d20478e140b1f93ee74d98de5048083f6d3")
			echo "Created super user:  unitrends / B3\$afe"
			;;
		"7c097237dfd41946593a0e6ce6ae40c0d734d868")
			echo "Created super user:  unitrends / unitrends"
			;;
		"38776419428ba4444f1d9b9f7aeda5623b7e5bbf")
			echo "Created super user:  unitrends / unitrends1"
			;;
		*)
			echo "User unitrends has an unexpected password."
	esac
}

is_scheduled(){
	debug "is_scheduled"
	shift
	SEARCHSTRING="${*}"
	QUERY="select schedule_id as sid, enabled, name as schedule, n.node_no as nno, i.instance_id as iid, node_name as client, key1, key2 from bp.schedule_client_assoc a join bp.application_instances i using(instance_id) join bp.schedules s using(schedule_id) join bp.nodes n using(node_no)"
	debug "QUERY=${QUERY}"
	psql bpdb -U postgres -c "${QUERY}" | egrep -i "nno.*iid|---|${SEARCHSTRING}"
}

draas_dashboard(){
	debug "draas_dashboard"
	sh ${0} --jobs
	echo "DRaaS daemons:"
	ps -eo pid,etime,args | egrep "tasker|replica_daemon|restore_daemon|rdrd.exe|rdrjob.exe" | egrep -v "grep|lmmgr|curl" | sed "s/^/\t/g"
	echo
	QUERY="select count(*) from job"
	debug "QUERY=${QUERY}"
	NUM="$(psql rdrdb -A -t -c "${QUERY}")"
	if [[ "${NUM}" == "0" ]]; then
		echo -e "DCA Jobs:\t\t\tnone"
		echo
	else
		echo "DCA Jobs:"
		QUERY="select id, name as dca_job, is_enabled as enabled, suffix_name as suffix from job"
	        debug "QUERY=${QUERY}"
		psql rdrdb -c "${QUERY}"
	fi
	QUERY="select count(1) from bp.vm_replicas"
        debug "QUERY=${QUERY}"
	NUM="$(psql bpdb -U postgres -A -t -c "${QUERY}")"
	if [[ "${NUM}" == "0" ]]; then
		echo -e "VMware Replicas:\t\tnone"
		echo
	else
		echo "VMware Replicas:"
		QUERY="select r.replica_id as rid, instance_id as iid, replica_name,valid,disabled,current_state as current,pending_state as pending, (select count(*) from bp.vm_replicas_queue where replica_id = r.replica_id and cur_restore_job_no is NULL) as pending_num, (select sum(total_megs) from bp.backups where backup_no in (select backup_no from bp.vm_replicas_queue where replica_id = r.replica_id and cur_restore_job_no is NULL)) as pending_mb, q.backup_no as restoring, last_message as message from bp.vm_replicas r join bp.last_replica_restores l using(replica_id) left join (select * from bp.vm_replicas_queue where cur_restore_job_no is NOT NULL) as q on r.replica_id = q.replica_id order by lower(r.replica_name) asc"
		debug "QUERY=${QUERY}"
		psql bpdb -U postgres -c "${QUERY}"
	fi
	NUM="$(crontab -l | egrep "Start-Replica.py|run_hyperv_replica_restores.sh" | grep -v "^#" | wc -l)"
	if [[ "${NUM}" == "0" ]]; then 
		echo -e "Hyper-V Replicas:\t\tnone"
		echo
	else
		echo -e "Hyper-V Replicas:"
		crontab -l | egrep "Start-Replica.py|run_hyperv_replica_restores.sh" | grep -v "^#"
		echo
	fi
	NUM="$(ls /var/lib/*.vm_info 2>/dev/null | wc -l)"
	if [[ "${NUM}" == "0" ]]; then
		echo -e "Instant Recovery:\t\tnone"
		echo
	else
		echo -e "Number of fuse processes:  \c"
		ps -leaf | grep fuse | grep -v grep | wc -l
		echo "Instant Recovery:"
		cat /var/lib/*.vm_info | grep vm_name | awk -F '>' ' { print $2 } ' | awk -F '<' ' { print $1 } ' | sed "s/^/\t/g"
		echo
	fi
	QUERY="select count(1) from bp.virtual_clients"
	debug "QUERY=${QUERY}"
	NUM="$(psql bpdb -U postgres -A -t -c "${QUERY}")"
	if [[ "${NUM}" == "0" ]]; then
		echo -e "Windows Replicas:\tnone"
		echo
	else
		echo "Windows Replicas:"
		QUERY="select virtual_id as vid, s.name as source, case when n.gcname is NOT NULL then n.gcname else n.node_name end as asset, vc.node_no as nno, cpus as cpu, memory as ram, valid, disabled, current_state as current, pending_state as pending, vm_name as name, ip_addr as ip, cur_backup_no as restoring, (select count(1) from bp.backups where backup_no in (select backup_no from bp.virtual_clients_queue where virtual_id = vc.virtual_id)) as pending_num, coalesce((select sum(total_megs) from bp.backups where backup_no in (select backup_no from bp.virtual_clients_queue where virtual_id = vc.virtual_id)), 0) as pending_mb, coalesce(last_checkpoint, 'n/a') as checkpoint, case when last_message not like 'Windows Replica restore job for % instance completed successfully%' then substring(last_message from 0 for 46) else '' end as msg from bp.virtual_clients as vc join bp.nodes n on vc.node_no = n.node_no left join bp.systems s on n.system_id = s.system_id left join bp.last_windows_replica_restores using(virtual_id) order by lower(vm_name) asc"
		debug "QUERY=${QUERY}"
		psql bpdb -U postgres -c "${QUERY}"
		TMP="$(ls -rt /usr/bp/logs.dir/restore_daemon-* 2>/dev/null | tail -n 1 2>/dev/null)"
		echo "Restore_daemon log tail (${TMP}):"
		if [[ -s "${TMP}" ]]; then
			tail -n 15 "${TMP}" 2>/dev/null | sed "s/^/  /g"
		else
			echo "Log ($({TMP}) either does not exist or has no size."
		fi
	fi
}

draas_watch_dashboard(){
	debug "draas_watch_dashboard"
	if [[ "$(rosVersion)" == "7" ]]; then
		watch -d -n 5 --color -- "sh ${0} --ddash"
	else
		watch -d -n 5 -- "sh ${0} --ddash"
	fi
}

show_seed_stats(){
        debug "show_seed_status()"
        for hac_pid in `pidof hactarToctar`
        do

             BACKUP_PATH=`lsof 2> /dev/null | grep " ${hac_pid} " | egrep 'backup_' | awk {'print $9'}`
             BACKUP_NO=`echo ${BACKUP_PATH} | awk -F'_' {'print $NF'}`

             DATE_REGEX='\d{4}(-|\/)((0[1-9])|(1[0-2]))(-|\/)((0[1-9])|([1-2][0-9])|(3[0-1]))(T|\s)(([0-1][0-9])|(2[0-3])):([0-5][0-9]):([0-5][0-9])'
             IMPORT_START=$(date +%s -d "$(psql bpdb -Atc "select messages from bp.backup_msg where backup_no='${BACKUP_NO}'" | grep -oP ${DATE_REGEX})")
             TOTAL_SIZE=$(($(psql bpdb -Atc "select total_megs from bp.backups where backup_no='${BACKUP_NO}'")/1024))

             if [[ $(ps aux | grep ${hac_pid} | grep -q ' \-F '; echo $?) == 0 ]]; then
                   debug "Backup is not being deduplicated"
                  
                   PROGRESS=$(($(ls -l ${BACKUP_PATH} | awk {'print $5'})/1024/1024))
             else
                   PROG=`grep "Deduplicating contents at " $( lsof 2> /dev/null | grep " ${hac_pid} " | egrep 'hactarToctar' | grep log | awk {'print $9'}) | tail -1 | sed 's/^.* \([^ ]\)/\1/g'`
                   PROG=`echo ${PROG:0:${#PROG}-1}`
             
                   #TOTAL_SIZE=$(($(ls -l ${BACKUP_PATH} | awk {'print $5'})/1024/1024))
                   PROGRESS="$(( ${PROG}/1024/1024/1024 ))"
             fi

             if [[ -z "${BACKUP_NO}" ]]; then
                echo "Unable to parse backup information for hactarToctar PID ${hac_pid}."
                continue
             fi

             if [[ ! -z "${QUERY}" ]]; then 
                QUERY="${QUERY} UNION ALL "
             fi
             
             QUERY="${QUERY}select n.gcname as client, a.key1, a.key2, a.key3, s.name as source, (select ${TOTAL_SIZE}) as total_gb, (select ${PROGRESS}) as progress_gb, (select to_timestamp(${IMPORT_START})) as import_start,(select ${BACKUP_NO}) as backup_no from bp.nodes n, bp.application_instances a, bp.backups b, bp.systems s where b.node_no=n.node_no AND b.instance_id=a.instance_id AND n.system_id=s.system_id AND b.backup_no='${BACKUP_NO}'"
        done
         
        if [[ -z "${QUERY}" ]]; then
             echo "There are no seed imports currently running."
        else
             psql bpdb -U postgres -c "${QUERY} ORDER BY import_start DESC"
        fi
}

seed_stats_watch_dashboard(){
         debug "seed_stats_watch_dashboard"
	if [[ "$(rosVersion)" == "7" ]]; then
	        watch -n 10 -d --color -- "sh ${0} --seedstats ${1} 2>&1"
	else
	        watch -n 10 -d -- "sh ${0} --seedstats ${1} 2>&1"
	fi
}

parse_ndmp_parser_logs(){
         debug "parse_ndmp_parser_logs"

         DATE=$(date '+%m-%d-%Y_%H_%M_%S');

         PARSER_LOG="/usr/bp/logs.dir/ndmp_index_parser.log";
         REPORT_DIR="/backups/samba/ndmp-parse-rpt";

         CSV="${REPORT_DIR}/ndmp-parse_rpt_${DATE}.csv";

         if [[ $(grep -qw "WARNING: Failed to add file" ${PARSER_LOG}*; echo $?) -eq 1 ]]; then
                echo "No problematic paths found in the parser log(s) - ${PARSER_LOG}";
                exit 2;
         fi

         mkdir -p ${REPORT_DIR};
         echo '"NDMP Volume","Problem Path (non-UTF8 & ...)"' > ${CSV};

         for PARSER_LOG in $(ls ${PARSER_LOG}*); do
             C=0; LC=0;

             echo -ne "Evalutating NDMP parser log ${PARSER_LOG}.....\r"
             TC=$(cat ${PARSER_LOG} | wc -l);

             while read LOG_LINE; do
                    C=$(($C+1)); LC=$((LC+1));
                    LINE=$(echo $LOG_LINE | sed 's/\[.*\] //g' | awk '{ if (!seen[$0]++ || $1 ~ /^DE/) print }');

                    if [[ ${C} == 10 || ${LC} == ${TC} ]]; then
                            echo -ne "Parsing NDMP parser log '${PARSER_LOG}' for problem paths..... [$LC of $TC]\r"
                            C=0;
                    fi

                    if [[ ${LINE} =~ 'DE FILESYSTEM'* ]]; then
                            if [[ -s ${LOG} ]]; then
                                    sort -o ${LOG} ${LOG};
                                    if [[ ${LOGS} != *${LOG}* ]]; then
                                          LOGS="${LOGS}           ${LOG}\n";
                                    fi
                            fi

                            FS=$(echo ${LINE} | awk -F'=' {'print $2'});
                            LOG="${REPORT_DIR}/ndmp-parse-rpt_${DATE}$(echo ${FS}| sed 's#/#_#g').txt";
                    elif [[ ${LINE} == *'WARNING: Failed to add file'* ]]; then
                            LINE=$(echo "${LINE}" | sed 's/WARNING: Failed to add file //g')

                            echo "${LINE}" >> ${LOG};
                            echo "\"${FS}\",\"${LINE}\"" >> ${CSV};
                    fi
             done < ${PARSER_LOG};
             echo "";
         done

         sort -o ${CSV} ${CSV};

         echo -e "\nReport Path: ${REPORT_DIR}"
         echo -e "Volume Logs:\n${LOGS}"
         echo "CSV Report: ${CSV}";
}

ARGS="${*}"
echo "${ARGS}" | grep -q -- "--debug"
if [[ "${?}" == "0" ]]; then
	DEBUG="YES"
fi
ARGS=`echo "${ARGS}" | sed "s/--debug//g"`
CMD=`echo "${ARGS}" | awk ' { print $1 } '`

debug "debug mode is enabled."

usage(){
	debug "usage"
	echo -e "version: 3.0.0

Usage: sh support.sh <arguments>

--ahc							Archive History Chart
--ahi <archive_set_id>					Archive History Item for specified archive_set_id
--bhc [<see_options>]					Backup History Chart with filter and sort options
		Options:
				-c			Comma delimited list (CDL) of bp.nodes.node_name values
				--gc			CDL of bp.nodes.gcname values
				-t			CDL of bp.backups.type values
				-s			CDL of bp.systems.name values
				-i			A single bp.application_instances.key1 or bp.application_instances.key2 value
				-b			CDL of backup_no values
				--todo			Idea is to show which bp.application_instances have the most recent backup is a failed backup
				--iid			CDL of bp.application_instances.instance_id values
				--meta	    	    	Show System Metadata backups
				--nopurge		Do not show any purged backups
				--success		Show only successful backups
				--active		Show only active backups
				--failed		Show only failed backups
				-e			Show \"extra\" columns - dedup, hash, comp, enc
				--synth			Show only synthesized backups
				--nosynth		Do not show synthesized backups
				--sort			Sort by CDL of columns using sort order, such as \"lower(n.node_name),lower(i.key1),lower(i.key2) asc\"
				--limit			Limit results to first X rows
				--stats			Show \"extra\" columns - MBps (MB/s), MBpm (MB/min), Fpm (Files/min), and Ambpf (Average MB/file)
				--bend			Show \"extra\" column bend (Backup end time)
				--csv			Display output as CSV rather than as a table
				--last24		Show only backups that started in the last 24 hours
				--lasth			Show only backups that started in the last X hours
				--lastd			Show only backups that started in the last X days
				--noactive		Do not show active backups
				--cols			Show only specified CDL of column names that would be otherwise presented using the other combinations of flags and parameters specified
				--extra			Show \"extra\" columns as specified in CDL that are available in tables bp.backups, bp.nodes, and bp.application_instances
				--imported		Show \"extra\" column imported that is a boolean describing the backup as imported or not
				--todo			Show only the latest backup for each instance where that backup is a failed backup.  In theory, this is the list of backup items that still require fixing.
				--newer <#>		Show only backups where the backups where backup_no > #
--bhi <backup_no>					Backup History Item
--bstats <num_days>					Show number of active, successful and failed backups for TODAY and the (num_days-1) days prior
--capacity						Capacity Report
--changerate <#days>					Change rate for last X days, or 1 (default)
--uniquechange <#days>                          	Unique data change rate for the last X days, or 7 (default)
--dbcancel <pid>					Cancel Postgres connection having PID <pid>
--dbpids						Show all database related PIDs that are not <IDLE>, and show running ANALYZE, VACUUM, or autovacuum queries
--dbstats						Return database maintenance status from the last five attempts
                Options:
                                --failed        	Show only tables for which maintenance has failed.
                                -e              	Show maintenance status for all tables
--dbterm <pid>						Terminate Postgres connection having PID <pid>
--dedup	<x>						Show latest dedup stats table entriee where <x> is the number of entires to list.
--download						Refresh this script
--elk							Validate ELK configuration and connectivity to ELK servers.
--extract <backup_no>					Extract all files from backup_no to /backups/samba/support_restore/<backup_no>/, or extract only inclusion list as specified in /tmp/infile.txt
--files <backup_no>					Show all files, time modified and size (KB) for items in backup <backup_no>
--gc <date>						Gantt Chart of all backups run on date specified using format MM/DD/YYYY
--getlogs <lin|win> <node_name> [evtx]			Queue a selective backup to get logs from client
-?|--help						Show this usage
--iotest						Use fio to test performance for /usr/bp/data/
--ipmi <get|clear>					Either show all IPMI alarms or clear all IPMI alarms using ipmiutil
--is-scheduled						Check for string as a key1, key2 or node_name for any item that is scheduled for backup.
--jobs							Show all running non-synthetic backups
--ndmparse                                      	Generate report of NDMP backups with files containing non-UTF8 and other DB unfriendly characters in /backups/samba/ndmp-parse-rpt, using existing ndmp_index_parser logs.
--openvpn_portquery <host> <port_number>        	Query OpenVPN UDP port to verify availability.
--pgdb							Remove /root/pgdb.hotfix.8.1.0-1, and then download a fresh copy
--pids							Show all BP PIDs based on bp.pids
--ractive						Show active replications table
--rcalculator						Show all date for use in replication calculator spreadsheet
--rcapacity						Target ONLY - Show replication capacity chart.  Creates /usr/bp/reports.dir/replication_capacity.csv and /usr/bp/reports.dir/replication_capacity-summary.rpt
--rconfig						Show replication configuration based on /usr/bp/bpinit/master.ini
--rdash <#>						Show replication dashboard 1 time, limiting history and queue to X rows each
--rdelete <failed|all_failed|all>			Delete from bp.sds_stats only rows that are failed and have not since replicated successfully, all failed records, or all records
--repeat <job_no>					Use /usr/bp/lists.dir/profile_submit_j<job_no>.spr to re-queue backup using /usr/bp/bin/bpr
--promote <instance_id>					Insert into bp.application_properties and set prop_key='promote_next'.
--promoted                                              Show instances configured to be promoted to a full upon next backup.
--retention						Show retention report similar to legacy UI's page that shows actual days of retention
--rhist	<#>						Show replication history, most recent X completions based on bp.sds_stats.done_epoch
--rlog <pid>						Use VI to edit log for VCD or VAULTSERVER log using file descriptor for PID <pid>
--rpids							Show running replication related PIDs
--rpms							List and verify all unitrends-* RPMs
--rqueue <#>						Show pending replication jobs, first upcoming X rows in bp.replication_queue based on position
--rstats						Show replication statistics.  The same data points are included in the replication dashboard.
--rpaused						Show replication queue items for paused instances (status=128).
--rwdash <#>						Show replication dashboard using rhist and rqueue for number of rows <#> where we watch the output for all updates
--search <backup_no> <search_string>			Search backup_no for search_string in the bp.backup_files.name
--seedstats                                     	Show seed import progress for imports that are currently running.
--shc [--id <schedule_id>] [--name <schedule_name>]	Schedule History Chart
--table	<table>						Select * from bp.<table>
--wtable <table>                                	Select * from bp.<table> where we watch the output for all updates
--treesize <backup_no> <start_dir>			Show treesize (files and sizes plus a summary) for all items under <start_dir> in <backup_no> from bp.dirs joined to bp.backup_files
--updatedb <backup_no>					Use /usr/bp/bin/updatedb to reload filenames for backup from the backup file corresponding to backup_no
--version						Show (/usr/local/bin/dpu version) and (rpm -qa | grep ^unitrends | sort -n)
--wjobs							Show a repeating watch of the bp.jobs information
--wseedstats                                    	Show a repeating watch of seed import progress statistics
" | more
	exit 1
}

case ${CMD} in
	-?|--help)
		usage
		;;
	--bhc)
		backup_history_chart ${ARGS}
		;;
	--bhi)
		backup_history_item ${ARGS} | more
		;;
	--search)
		backup_search ${ARGS}
		;;
	--treesize)
		tree_size ${ARGS}
		;;
	--shc)
		schedule_history ${ARGS}
		;;
	--gc)
		gantt_chart ${ARGS}
		;;
	--wjobs)
		watch_jobs
		;;
	--jobs)
		jobs
		;;
	--pids)
		pids
		;;
	--getlogs)
		get_client_logs "${2}" "${3}" "${4}"
		;;
	--rwdash)
		replication_watch_dashboard "${2}"
		;;
	--rdash)
		replication_dashboard "${2}"
		;;
	--ractive)
		replication_active
		;;
	--rhist)
		replication_history ${2}
		;;
	--rqueue)
		replication_queue ${2}
		;;
	--rpids)
		replication_pids
		;;
	--rconfig)
		replication_config
		;;
	--rlog)
		replication_log ${ARGS}
		;;
	--extract)
		backup_extract ${ARGS}
		;;
	--updatedb)
		backup_updatedb ${ARGS}
		;;
	--rpms)
		rpms
		;;
	--version)
		version
		;;
	--download)
		download
		;;
	--upload)
		upload
		;;
	--table)
		show_table ${ARGS}
		;;
        --wtable)
                watch_table ${2}
                ;;
	--retention)
		get_retention_limits ${ARGS}
		;;
	--capacity)
		get_capacity ${*}
		;;
	--ipmi)
		ipmi ${ARGS}
		;;
	--changerate)
		change_rate ${ARGS}
		;;
	--uniquechange)
		unique_change ${ARGS}
		;;
	--rdelete)
		replication_history_delete ${ARGS}
		;;
	--bstats)
		backup_statistics ${ARGS}
		;;
	--rpaused)
		replication_paused
		;;
	--rstats)
		replication_statistics ${ARGS}
		;;
	--ahc)
		archive_history_chart ${ARGS}
		;;
	--ahi)
		archive_history_item ${ARGS}
		;;
	--rcapacity)
		replication_capacity
		;;
	--rcalculator)
		replication_calculator
		;;
	--pgdb)
		pgdb
		;;
	--dbpids)
		show_database_processes
		;;
	--dbcancel)
		cancel_database_process ${ARGS}
		;;
        --dbstats)
                db_maint_status ${ARGS}
                ;;
	--dbterm)
		terminate_database_process ${ARGS}
		;;
	--iotest)
		io_test
		;;
	--files)
		show_files ${ARGS}
		;;
	--repeat)
		repeat_job_no ${ARGS}
		;;
        --promoted)
                show_promoted_instances
                ;;
	--promote)
		promote_next_backup ${ARGS}
		;;
        --openvpn_portquery)
                openvpn_portquery ${ARGS}
                ;;
	--elk)
		check_elk
		;;
        --dedup)
                dedup_stats ${ARGS}
                ;;
	--super)
		create_super_user
		;;
        --seedstats)
		show_seed_stats
		;;
        --wseedstats)
                seed_stats_watch_dashboard "${2}"
                ;;

	--is-scheduled)
		is_scheduled ${ARGS}
		;;
	--ddash)
		draas_dashboard ${ARGS}
		;;
	--dwdash)
		draas_watch_dashboard ${ARGS}
		;;
	--dhc)
		dca_history_chart ${ARGS}
		;;
        --ndmparse)
                parse_ndmp_parser_logs ${ARGS}
                ;;
	--wput)
		wput ${ARGS}
		;;
	*)
		usage
		;;
esac
exit 0
