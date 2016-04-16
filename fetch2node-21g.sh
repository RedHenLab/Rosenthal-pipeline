#!/bin/bash -xv
#
# On a compute node, copy a file from a recording machine
#
# Called by node-daemon-local.sh
#
# Written 2013-10-22 FFS
#
# Changelog
#
#	2016-02-16 Groeling fork for Archive files -- fetch2node-00g
#	2015-11-29 No longer necessary to forward ports
#	2014-05-29 Try login node hoffman2.idre.ucla.edu first
#       2014-01-25 Include ~/tv/pond as potential location for half-processed files (from an interrupted process) (not implemented yet)
#	2014-01-02 Try -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no (seems to work fine)
#       2013-12-03 Use $HOME/tv2 instead of $SCRATCH
#	2013-11-18 Fetch the text file for the ocr tree
#       2013-11-16 Random port in top half of IANA range; seed with $$
#       2013-11-15 Option to distribute load to four sources
#
# Beware that this script is called every few seconds if fetch-daemon-local.sh
# is running at full speed. Make sure to save to a different name and test first!
# Failure in this script could cause a mess and slow down a lot of jobs.
#
# -------------------------------------------------------------------------------

# Script name
SCRIPT=`basename $0`

# Help screen
if [ "$1" = "-h" -o "$1" = "--help" -o "$1" = "help" ] ; then
  echo -e "\n\tOn a compute node, copy a file from a digitizing hub or directly from a laptop."
  echo -e "\n\tExample:"
  echo -e "\n\t\t$SCRIPT wd2 2006-05-02_0000_US_Archive_V2_MB10_VHS6_H12_MS.mpg"
  echo -e "\n\tDefault extension is mpg.\n"
  echo -e "\tThe script is called from a compute node by node-daemon-local.sh,"
  echo -e "\twhich gets the name of the remote host from the .len file in ~/tv2/pond.\n"
   exit
fi

# Get the name of the recording system to copy from
if [ -z "$1" ]
  then echo -e "\n\tFor usage, see $SCRIPT -h\n" ; exit
  else RHOST="$1"
fi

# Start time
START=$( date +%s )

# Translate to IP
ca=164.67.183.179
roma=164.67.183.182
wd1=128.97.202.227
wd2=128.97.202.224
wd3=128.97.202.226
wd4=128.97.202.238

# Define the storage and source trees
case $RHOST in
 ca   ) TREE="/mnt/ifs/NewsScape/Rosenthal" ;;
 roma ) TREE="/mnt/ifs/NewsScape/Rosenthal" ;;
 *    ) TREE="/mnt/HD/HD_a2/Comm/VHS"       ;;
esac

# Get the file name
if [ -z "$2" ]
  then echo -e "\n\tFor usage, see $SCRIPT -h\n" ; exit
  else FIL="$2"
fi

# Default extension
if [ ${FIL#*.} = $FIL ]
  then EXT=mpg
  else EXT=${FIL#*.} FIL=${FIL%.*}
fi

# Host
HOST="$( hostname -s )"

# Home directory
HOME=/u/home/g/groeling

# Local executables
LBIN=$HOME/bin

# Scratch directory
SCRATCH=/u/scratch/g/groeling

# Primary reservations
POND=$HOME/tv2/pond

# Make sure the local directory exists
WORK=/work/pond ; mkdir -p $WORK

DAYS=$HOME/tv2/day

# Failed conversions (not used in this script)
DROPS=/u/scratch/g/groeling/drops

# Log directory
LOGS=$HOME/tv2/logs

# Current jobs
MYJOBS=$HOME/tvspare/myjobs

# Generate random numbers in a range (e.g., roll_die 10)
function roll_die() {

  # capture parameter
  declare -i DIE_SIDES=$1

  # check for die sides
  if [ ! $DIE_SIDES -gt 0 ]; then
    # default to 6
    DIE_SIDES=6
  fi

  # echo to screen
  echo $[ ( $RANDOM % $DIE_SIDES )  + 1 ]
}

# Get the current grid engine queue number
QNUM="$( echo $PATH | sed -r 's/.*\/work\/([0-9]{3,7})\..*/\1/' )"

# On exclusive nodes, use the process list
if ! [[ "$QNUM" =~ ^[0-9]+$ ]] ; then QNUM="$( ps x | grep job_scripts | grep -v grep | tail -n 1 | sed -r 's/.*\/([0-9]{3,8})/\1/' )" ; fi

# On interactive nodes, infer the queue number from the node name
if ! [[ "$QNUM" =~ ^[0-9]+$ ]] ; then QNUM="$( myjobs | grep `hostname -s` | egrep "QRLOGIN|$SCRIPJ" | tail -n 1 | cut -d" " -f1 )" ; fi

# If the queue number is not found, use zero -- or just exit? Normally this means you don't have rights.
if ! [[ "$QNUM" =~ ^[0-9]+$ ]] ; then QNUM=000000 ; echo -e "\n\tCannot find a queue number\n" ; fi #exit ; fi

# Check the space (never been an issue)
if [ "$( df /work | tail -1 | awk '{print $4}' )" -lt "50000000" ]
  then echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \t< 50GB     \t$FIL" | tee -a $LOGS/reservations.$( date +%F )
    echo -e "\n$HOST:/work has less than 50GB of free space\n" | mail -s "Space low on Hoffman2 $HOST" groeling@mail ; sleep 3600 ; exit
fi

# Number of raw video files on this node
VIDS="$( ls -1 $WORK/*$EXT 2>/dev/null | wc -l )"

# Number of our processes running on this node (you could use $$ to identify the parent process)
DAES="$( ps x | grep node-daemon-local.sh | grep -v grep | wc -l )"

# Prevent fetching to a crowded node (likely due to a cleanup problem that needs attention)
if [ "$( ls -1 $WORK/*$EXT 2>/dev/null | wc -l )" -gt "$[DAES+2]" ] ; then RDM=`roll_die 10`
  if [ "$RDM" = 3 ] ; then echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \tCROWDED \t$DAES:$VIDS \t\t$FIL" | tee -a $LOGS/reservations.$( date +%F ) ; fi
  sleep 68.48 ; rm -rf $POND/$FIL.fetched ; rm -rf $POND/$FIL.reserved ; rm -rf $WORK/$FIL.fetched ; rm -rf $WORK/$FIL.reserved ; exit
fi

# Limit file downloads to one per node (this is a bit complicated to achieve and not really needed)
#if [ "$( ps x | grep bash | grep -v grep | grep _Archive_ | grep -c fetch2node )" -ge "2" ] ; then exit ; fi

# Strip path and extension
FIL=${FIL##*/} FIL=${FIL%.*}

# Generate the tree
DDIR="$(echo $FIL | sed -r 's/([0-9]{4})-([0-9]{2})-([0-9]{2}).*/\1\/\1-\2\/\1-\2-\3/')"

# Create the local day directory in the shared file system
mkdir -p $DAYS/$DDIR

# Create the local day directory on the compute node
mkdir -p /work/day/$DDIR/

# Skip existing
if [[ "$( ls -1 $WORK/$FIL.{mpg,mp4.done} 2>/dev/null )" != "" || -f $WORK/$FIL.$EXT ]] ; then exit ; fi
if [[ "$( ls -1 $POND/$FIL.{fetched,mp4.done} 2>/dev/null )" != "" ]] ; then echo -e "\n\t$FIL is already processing\n" ; exit ; fi
if [  "$( ls -1 $DAYS/$FIL.{mp4.done,srt,txt3} 2>/dev/null )" != "" ] ; then exit ; fi

# Attempt to reserve the file on the WDM (remote reservation -- do we need it? Not working yet)
#if [ "$( ssh $RHOST "mkdir $TREE/$FIL.fetched 2> /dev/null; echo $?" )" = "0" ]
#  then Trace="$( echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \t${RHOST^^} ${TREE##*/} \t$DAES jobs   \t$FIL.$EXT" )"
#     ssh $RHOST "echo $Trace" > $TREE/$FIL.fetched/$QNUM ; echo "$Trace" | tee -a $LOGS/reservations.$( date +%F )
##    ssh $RHOST "echo $Trace-e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \t${RHOST^^} ${TREE##*/} \t$DAES jobs   \t$FIL.$EXT" > $TREE/$FIL.fetched/$QNUM"
#  else echo -e "\n\t$FIL is already reserved for compression.\n" ; exit
#fi

# Attempt to reserve the file for fetching
if [ "$( mkdir $POND/$FIL.fetched 2> /dev/null; echo $? )" = "0" ]
  then echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \t${RHOST^^} ${TREE##*/} \t$DAES jobs   \t$FIL.$EXT" > $POND/$FIL.fetched/$QNUM
  else echo -e "\n\t$FIL is already reserved for frame extraction and OCR.\n" ; exit
fi

# Verify that the file is reserved (the reservation may have been deleted by the fetch daemon) -- you should check this belongs to the current node
if [ "$( mkdir $POND/$FIL.reserved 2> /dev/null; echo $? )" = "0" ]
  then echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT:0:11} \t$QNUM\t$HOST \tLocking \t$RHOST \t$FIL" > $POND/$FIL.reserved/$QNUM
fi

# Verify
if [ ! -d "$POND/$FIL.fetched" ] ; then echo -e "\tUnable to reserve a file from the queue on ~/tv2/pond" ; exit ; fi

# Local reservation (less likely to be missing -- use to verify?)
if [ "$( mkdir $WORK/$FIL.reserved 2> /dev/null; echo $? )" = "0" ]
  then echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT:0:11} \t$QNUM\t$HOST \tLocking \t$RHOST \t$FIL" > $WORK?$FIL.reserved/$QNUM
fi

# A. Direct copy

# Try twice -- or not? If it's busy, give up at once?
n=0 ; while [ $n -le 1 ] ; do n=$[n+1]

  # Can you access the host directly?
  if [ "$( ssh $RHOST "ifconfig | grep ${!RHOST}" )" = "" ] ; then break ; fi

  # Hold off if the WDM is already busy copying -- and check after a randomized interval to avoid simultaneous slot grabs
  DNLS=$( ssh $RHOST "ps x | grep rsync | grep -v grep | grep -cv sh\ -c" )

  if [ $DNLS -lt 3 ]
    then sleep `roll_die 265` ; DNLS=$( ssh $RHOST "ps x | grep rsync | grep -v grep | grep -cv sh\ -c" )
      if [ $DNLS -lt 3 ]
        then echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \t${RHOST^^} ${TREE##*/} \t$DNLS downloads $DAES\t$FIL.$EXT" | tee -a $LOGS/reservations.$( date +%F )
          rsync $RHOST:$TREE/$FIL.$EXT $WORK -a 2>/dev/null
        else echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \t${RHOST^^} Still busy \t$DNLS downloads   \t$FIL.$EXT" | tee -a $LOGS/reservations.$( date +%F )
          rm -r $POND/$FIL.fetched ; rm -r $POND/$FIL.reserved ; exit
      fi
    # If busy, show a message occasionally -- calibrate as needed
    else RDM=`roll_die 10`
      if [ "$RDM" = 5 ] ; then echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \t${RHOST^^} Busy     \t$DNLS downloads   \t$FIL.$EXT" | tee -a $LOGS/reservations.$( date +%F ) ; fi
      rm -r $POND/$FIL.fetched ; rm -r $POND/$FIL.reserved ; exit
  fi

  # Verify file or try again
  if [ -f "$WORK/$FIL.$EXT" ] ; then break ; else sleep 60 ; fi

done

# Length of download process
DNLD="$(date -ud "+$[`date +%s`-START] seconds"\ $(date +%F) +%H:%M:%S)"

# Completion time
if [ -f "$WORK/$FIL.$EXT" ]
  then echo -e "\n\tFetched $FIL.$EXT"
    echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \t${RHOST^^} Dd time $[DNLS+1] \t$DNLD \t$FIL.$EXT" | tee -a $LOGS/reservations.$( date +%F )
  else echo -e "\n\tFailed to fetch $FIL.$EXT" ; rm -r $POND/$FIL.fetched ; rm -r $POND/$FIL.reserved
    echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \t${RHOST^^} Failed    \t$DNLD \t$FIL.$EXT" | tee -a $LOGS/reservations.$( date +%F )
fi

# The node daemon will try other .len files
exit

# B. Use port forwarding if direct copy fails -- this seems to not be working, so disabled for now

# Seed RANDOM to the current PID (max 32768, cat /proc/sys/kernel/pid_max)
RANDOM=$$

# Random port to forward ($RANDOM is 0 - 32767 and ports is 1024 - 65535, upper range recommended for dynamic use)
RPORT=$[ RANDOM + 32767 ]

# Make multiple attempts -- the generic name does not work from all compute nodes
for LOGIN in hoffman2.idre.ucla.edu login2 login3 login4 login1 hoffman2.idre.ucla.edu login2 login3 login4 login1 ; do

  # Forward the port and background it (from Shao-Ching Huang)
  ssh -N -L $RPORT:${!RHOST}:22 $LOGIN 2>/dev/null &

  # Give the port time to settle
  sleep 10

  # Get the ID of the forwarding process
  PID=$!

  # Verify port forward (not reliable since even failed attempts show up briefly)
  if [ "$( ps x | grep $PID | grep "$RPORT:${!RHOST}:22 $LOGIN" | grep -v grep )" = "" ] ; then continue ; fi

  # Hold off if the WDM is already busy copying -- and check twice at a randomized interval to avoid simultaneous slot grabs
  if [ $( ssh -p $RPORT localhost "ps x | grep rsync | grep -v grep | grep -cv sh\ -c" ) -lt 3 ] ; then sleep `roll_die 265`
    if [ $( ssh -p $RPORT "ps x | grep rsync | grep -v grep | grep -cv sh\ -c" ) -lt 3 ]
      then echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \t${RHOST^^} ${TREE##*/} \t$DAES jobs   \t$FIL.$EXT" | tee -a $LOGS/reservations.$( date +%F )
	rsync -e "ssh -p $RPORT" localhost:/$TREE/$FIL.$EXT $WORK -a 2>/dev/null
      else continue
    fi
  fi

  # Close the port
  kill $PID

  # Verify file or try again
  if [ -f "$WORK/$FIL.$EXT" ] ; then
    echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \t${RHOST^^} ${TREE##*/} \t$DAES jobs   \t$FIL.$EXT" | tee -a $LOGS/reservations.$( date +%F )
    break
  fi

done

# Verify file and release if not (node-daemon-local.sh may also do this)
if [ ! -f "$WORK/$FIL.$EXT" ]
  then rm -r $POND/${FIL%.*}.fetched ; echo -e "\n\tFailed to fetch $FIL.$EXT\n"
  else echo -e "\n\tFetched $FIL.$EXT"
fi

# EOF
