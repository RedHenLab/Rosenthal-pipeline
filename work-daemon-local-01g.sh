#!/bin/bash
#
# Submit jobs to the grid engine for files copied to the local compute nodes
#
# Written on 18 November 2013, FFS
#
# Changelog
#
#	2016-03-03 Adapt to Archive files for user groeling
#	2014-10-21 No job requests if no .q files waiting
#       2013-12-03 Use $HOME/tv2 instead of $SCRATCH
#	2013-11-18 Forked from fetch-daemon-local.sh
#
# ---------------------------------------------------------------------------------

# Manual override for home directory (normally not needed)
#HOME=/u/home/g/groeling

# Manual override of scratch directory (not needed)
#SCRATCH=/u/scratch/f/groeling

# Script name
SCRIPT=`basename $0`

# Help screen
if [ "$1" = "-h" -o "$1" = "--help" -o "$1" = "help" ]
 then echo -e "\n\t$SCRIPT\n"
  echo -e "\t\tSubmit job requests to the grid engine for files copied to the local compute nodes.\n"
  echo -e "\tSyntax:\n"
  echo -e "\t\t$SCRIPT [<maximum number of grid engine jobs>]\n"
  echo -e "\tThe default maximum is 4 (3 on weekends).\n"
  echo -e "\tYou can interrupt the script when paused in the left margin,"
  echo -e "\tor soft terminate the grid engine jobs by hiding all the .len files.\n"
    exit
fi

# Log directory
LOGS=$HOME/tv2/logs

# The maximum number of jobs -- potentially a few more on the weekend, but beware above 450
if [ -z "$1" ]
  then if [ $( date +%u ) -ge 6 ] ; then MAXjobs=3 ; else MAXjobs=4 ; fi
  else MAXjobs="$1"
fi

# Initialize
#LNODS=a LIDLE=a LPEND=a LWAIT=a LFTC=a

# Welcome
echo -e "\n\tThis daemon submits new grid engine job requests when needed ...\n"

# Daemon loop -- exit once a day to enable local e-mail monitoring
# while [[ "$( date +%H:%M )" != 12:5* ]] ; do

# Or not
while true ; do

  # Get the number of video files waiting to be processed
  LEN="$( ls -1 $HOME/tv2/pond/*.len 2>/dev/null | wc -l )"
  FTC="$( ls -1d $HOME/tv2/pond/*.fetched 2>/dev/null | wc -l )"
  WAIT=$[ LEN - FTC ]

  # Count active and pending jobs
  NODS="$( qstat -s r -u groeling | grep -c groeling )"
  PEND="$( qstat -s p -u groeling | grep -c groeling )"

  # Idle nodes
  IDLE=$[NODS-FTC]

  # Update the status line if there's a change
  if [[ "$LNODS" = $NODS && "$LIDLE" = $IDLE && "$LPEND" = $PEND && "$LWAIT" = $WAIT && "$LFTC" = $FTC ]] ; then echo -n
    else echo -e "\t$( date +%Y-%m-%d\ %H:%M:%S ) \t$NODS\t$IDLE idle nodes, $PEND jobs pending, $WAIT files waiting and $FTC files processing"
      LNODS=$NODS LIDLE=$IDLE LPEND=$PEND LWAIT=$WAIT LFTC=$FTC
  fi

  # Terminate jobs if all nodes are idle
  if [ $IDLE -eq $NODS -a $WAIT -eq 0 ] ; then
    for JOB in `qstat -s r -u groeling | grep groeling | cut -d" " -f1` ; do qdel $JOB &>/dev/null
      #echo -e "\n\t$( date +%Y-%m-%d\ %H:%M:%S ) \t$JOB \tdeleted"
    done
  fi

  # Hold off if the number of files waiting is no greater than pending jobs, or if there are jobs idling
  if [ $WAIT -le $PEND -o $IDLE -gt 0 ] ; then sleep 123 ; continue ; fi

  # Today's date for lists and log files
  TAY=$( date +%F )

  # Add to the fetch-daemon-local log?
  #CLOG=$LOGS/completed.$TAY-fetched-local-$$.log

  # Or use the reservations log?
  #CLOG=$LOGS/reservations.$TAY.log

  # Or a new log?
  CLOG=$LOGS/work-daemon-local.$TAY.log

  # Read the job list into an array -- do it a few times if you get zero
  #MYjobs=( `/u/local/bin/qstat | grep groeling 2>/dev/null` )
  OFS=$IFS IFS=$'\n' ; MYjobs=( `myjobs | grep groeling 2>/dev/null` ) ; IFS=$OFS
  n=0 ; while [ "${#MYjobs[@]}" = "0" -a $n -lt 3 ] ; do n=$[ n + 1 ]
    echo -n . ; OFS=$IFS IFS=$'\n' ; MYjobs=( `myjobs | grep groeling 2>/dev/null` ) ; IFS=$OFS ; sleep 5.55 ; echo -en "\r "
  done

  # Count the other jobs pending (there are no other jobs for the Rosenthal pipeline -- currently useless but harmless)
  WaitOther="$( printf -- '%s\n' "${MYjobs[@]}" | egrep -v "all-4c-24l|hp-14d" | grep -c qw )"
  #WaitOther="$( printf -- '%s\n' "${MYjobs[@]}" | grep -c qw )"

  # Hold off if other jobs are pending (does not apply to the Rosenthal pipeline, but harmless)
  while [ "$WaitOther" -gt "0" ] ; do echo -en "\r\t$( date +%Y-%m-%d\ %H:%M:%S ) \t$ArchiveJobs\t$WaitOther Non-Archive jobs pending" ; sleep 52.60
    LMYjobs=$MYjobs LWaitOther=$WaitOther ; OFS=$IFS IFS=$'\n' ; MYjobs=( `myjobs | grep groeling 2>/dev/null` ) ; IFS=$OFS
    while [ "${#MYjobs[@]}" = "0" ] ; do echo -n . ; OFS=$IFS IFS=$'\n' ; MYjobs=( `myjobs | grep groeling 2>/dev/null` ) ; IFS=$OFS ; sleep 55.55 ; echo -en "\r " ; done
    WaitOther="$( printf -- '%s\n' "${MYjobs[@]}" | egrep -v "all-4c-24l|hp-14d" | grep -c qw )"
    ArchiveJobs="$( printf -- '%s\n' "${MYjobs[@]}" | grep -v qw | grep -c all-4c-24l )"
    if [ "$MYjobs" != "$LMYjobs" -o "$WaitOther" != "$LWaitOther" ] ; then echo "" ; fi
  done

  # Count the running Archive jobs
  ArchiveJobs="$( printf -- '%s\n' "${MYjobs[@]}" | grep -v qw | grep -c all-4c-24l )" #; echo -e "\t$ArchiveJobs jobs running"

  # Count the pending jobs
  ArchiveWait="$( printf -- '%s\n' "${MYjobs[@]}" | grep all-4c-24l | grep -c qw )" n=0 #; echo -e "\t$ArchiveWait jobs pending"

  # Hold off if too many ArchiveJobs are already pending
  while [ "$ArchiveWait" -gt "2" -a $n -lt 10 ] ; do n=$[n+1]
    #echo -en "\r\t$( date +%Y-%m-%d\ %H:%M:%S ) \t$ArchiveJobs\t$ArchiveWait Archive jobs pending"  ; sleep 12.61
    LMYjobs=$MYjobs LArchiveWait=$ArchiveWait ; OFS=$IFS IFS=$'\n' ; MYjobs=( `myjobs | grep groeling 2>/dev/null` ) ; IFS=$OFS
    while [ "${#MYjobs[@]}" = "0" ] ; do echo -n . ; OFS=$IFS IFS=$'\n' ; MYjobs=( `myjobs | grep groeling 2>/dev/null` ) ; IFS=$OFS ; sleep 55.55 ; echo -en "\r " ; done
    unset ArchiveWait ; ArchiveWait="$( printf -- '%s\n' "${MYjobs[@]}" | grep all-4c-24l | grep -c qw )"
    unset ArchiveJobs ; ArchiveJobs="$( printf -- '%s\n' "${MYjobs[@]}" | grep -v qw | grep -c all-4c-24l )"
    if [ "$MYjobs" != "$LMYjobs" -o "$ArchiveWait" != "$LArchiveWait" ] ; then echo "" ; fi
  done

  # Count the non-Archive jobs pending
  WaitOther="$( printf -- '%s\n' "${MYjobs[@]}" | egrep -v "all-4c-24l|hp-14d" | grep -c qw )"

  # Count the running Archive jobs
  ArchiveJobs="$( printf -- '%s\n' "${MYjobs[@]}" | grep -v qw | grep -c all-4c-24l )" #; echo -e "\t$ArchiveJobs jobs running"

  # Initialize counter
  n=0 ; echo -en "\r "

  # Submit new grid jobs unless we have as many as we can handle
  if [[ $ArchiveWait -le 3 && $ArchiveJobs -le $MAXjobs && $n -le 3 ]] ; then n=$[n+1] ; sleep 0.62

    # Submit a quick succession of grid engine jobs, in case things are moving fast (not needed for Rosenthal pipeline)
    #if [[ $WaitOther -le 1 && $ArchiveWait -le 1 && $[ MAXjobs - ArchiveJobs ] -ge 3 ]]
    #  then for i in {1..3} ; do sleep 0.63 ; qsub $HOME/bin/all-4c-24l.cmd &>/dev/null #; ArchiveJobs=$[ArchiveJobs+1]
    #    echo -en "\t$( date +%Y-%m-%d\ %H:%M:%S ) \t$ArchiveJobs \tNew Archive job request    \n " | tee -a $CLOG ; done
    #fi

    #unset MYjobs ; unset WaitOther ; OFS=$IFS IFS=$'\n' ; MYjobs=( `myjobs | grep groeling 2>/dev/null` ) ; IFS=$OFS
    #while [ "${#MYjobs[@]}" = "0" ] ; do echo -n . ; OFS=$IFS IFS=$'\n' ; MYjobs=( `myjobs | grep groeling 2>/dev/null` ) ; IFS=$OFS ; sleep 55.55 ; echo -en "\r " ; done

    # Submit a single job
    if [[ $WaitOther -le 1 && $ArchiveWait -le 3 && $MAXjobs -gt $[ArchiveJobs + ArchiveWait] ]]
      then qsub $HOME/bin/all-4c-24l.cmd &>/dev/null
        #echo -en "\n\t$( date +%Y-%m-%d\ %H:%M:%S ) \t$ArchiveJobs \tNew Archive job request" | tee -a $CLOG
      else echo ""
    fi

  fi

  echo -en "\r " ; echo -en "." ; read -t 10.66 -n 1 -s ; echo -en "\r "

done

# EOF
