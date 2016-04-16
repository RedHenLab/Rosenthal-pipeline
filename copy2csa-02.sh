#!/bin/bash
#
# On a compute node, copy a file to a NewsScape storage machine
#
# Written 2013-10-22 FFS
#
# Changelog
#
#	2016-02-24 Set permissions on remote file
#	2016-02-17 Groeling version for Archive files only
#	2016-02-12 Copy uncut digitized files to /mnt/ifs/NewsScape/Rosenthal
#	2015-11-29 No longer necessary to use port forwarding
#	2014-05-29 Try hoffman2.idre.ucla.edu first
#       2013-12-03 Use $HOME/tv2 instead of $SCRATCH
#	2013-12-03 Try -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no
#
# ----------------------------------------------------------

# Script name
SCRIPT=`basename $0`

# Help screen
if [ "$1" = "-h" -o "$1" = "--help" -o "$1" = "help" ] ; then
  echo -e "\n\tOn a compute node, copy a file or directory to storage."
  echo -e "\n\tSyntax:"
  echo -e "\n\t\t$SCRIPT <filename.ext>"
  echo -e "\n\tExample:"
  echo -e "\n\t\t$SCRIPT 2006-10-24_0000_US_Archive_V4_MB10_VHS6_H12_JN.mp4"
  echo -e "\tThe script outputs a last line of OK if the copying succeeds.\n"
   exit
fi

# Get the file name
if [ -z "$1" ]
  then echo -e "\n\tFor usage, see $SCRIPT -h\n" ; exit
  else FIL="$1"
fi

# Strip path and extension
FIL=${FIL##*/} EXT=${FIL#*.} FIL=${FIL%%.*}

# Verify extension
if [ "$EXT" = "$FIL" ] ; then echo -e "\n\tPlease give an extension.\n" ; exit ; fi

# Host
HOST="$( hostname -s )"

# Home directory
HOME=/u/home/g/groeling

# Local executables
LBIN=$HOME/bin

# Scratch directory
SCRATH=/u/scratch/f/groeling

# Primary reservations
POND=$$HOME/tv2/pond

# Log directory
LOGS=$$HOME/tv2/logs

SDIR=/work/pond ; mkdir -p $SDIR

# Get the current grid engine queue number
QNUM="$( echo $PATH | sed -r 's/.*\/work\/([0-9]{3,7})\..*/\1/' )"

# On exclusive nodes, use the process list
if ! [[ "$QNUM" =~ ^[0-9]+$ ]] ; then QNUM="$( ps x | grep job_scripts | grep -v grep | tail -n 1 | sed -r 's/.*\/([0-9]{3,8})/\1/' )" ; fi

# On interactive nodes, infer the queue number from the node name
if ! [[ "$QNUM" =~ ^[0-9]+$ ]] ; then QNUM="$( myjobs | grep `hostname -s` | egrep "QRLOGIN|$SCRIPJ" | tail -n 1 | cut -d" " -f1 )" ; fi

# If the queue number is not found, use zero -- or just exit? Normally this means you don't have rights.
if ! [[ "$QNUM" =~ ^[0-9]+$ ]] ; then QNUM=000000 ; echo -e "\n\tCannot find a queue number\n" ; fi #exit ; fi

# Skip completed (is this all we need?)
if [[ "$( ls -1 $POND/$FIL.$EXT-done 2>/dev/null )" != "" ]] ; then exit ; fi

# Welcome
echo -e "\tStarting $SCRIPT $FIL.$EXT $CSA $TREE on $HOST at $( date +%Y-%m-%d\ %H:%M:%S )\n" ; sleep 1

# Debug -- ssh version check
#ssh -V

# Generate the tree
DDIR="$(echo $FIL | sed -r 's/([0-9]{4})-([0-9]{2})-([0-9]{2}).*/\1\/\1-\2\/\1-\2-\3/')"

# Find the file
if [ -f "$FIL.$EXT" ] ; then F="$FIL.$EXT"
  elif [ -f "$SDIR/$FIL.$EXT" ] ; then F="$SDIR/$FIL.$EXT"
  elif [ -f "$POND/$FIL.$EXT" ] ; then F="$POND/$FIL.$EXT"
  else echo -e "\n\tNot finding $FIL.$EXT\n" ; exit
fi

# Try a couple of times
n=0 ; while [ $n -le 2 ] ; do n=$[n+1]

     TV=/mnt/ifs/NewsScape/Rosenthal ; ssh -q ca "mkdir -p $TV/$DDIR"
     rsync $F ca:$TV/$DDIR -aq --chmod=Fu=rw,Fg=r,Fo=r

     # Mark the file done on the source
     if [ -f $HOME/tv2/day/$DDIR/$FIL.len ] ; then WDM="$( cut -d" " -f1 $HOME/tv2/day/$DDIR/$FIL.len )"
       elif [ -f ${F%.*}.len ] ; then WDM="$( cut -d" " -f1 ${F%.*}.len )" ; fi
     if [ "$EXT" = "mp4" ] ; then ssh -q $WDM "touch /mnt/HD/HD_a2/Comm/VHS/mp4-only/$FIL.mp4.done" ; fi

     # Verify
     if [ "$( ssh -q ca "ls -ld $TV/$DDIR/$FIL.$EXT 2>/dev/null" )" != "" ]
        then echo -e "\nOK" ; break
        else echo -e "\nCopy to storage on IFS failed"
     fi

done

# EOF
