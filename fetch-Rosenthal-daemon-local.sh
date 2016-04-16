#!/bin/bash
#
# Download cutpoint files from Groeling's digitizing project as markers of files to be processed
#
# Written on 26 November 2015, FFS
#
# Changelog
#
#	2016-02-12 Forked from fetch-Redhen-daemon-local.sh -- customize for digitizing project
#	2015-11-26 Forked from fetch-daemon-local.sh
#       2013-12-03 Use $HOME/tv2 instead of $SCRATCH
#	2013-11-20 Separate out ocr sweep to a different script, sweep-daemon-local.sh
#	2013-11-19 Optimize and move queue refill to beginning
#	2013-11-18 Separate out job submission to a different script, work-daemon-local.sh
#	2013-10-23 Forked from fetch-daemon.sh
#
# ---------------------------------------------------------------------------------
#
# Notes for eyetv parsing
#
# *.eyetvr containst this metadata:
#
#        <key>actual duration</key>
#        <real>29698.0137</real>
#        <key>actual start time</key>
#        <date>2016-02-24T03:14:02Z</date>
#
#        <key>display title</key>
#        <string>2006-05-13_0000_US_Archive_V2_MB6_VHS8_H10_MS</string>
#
# So you could just grep Archive and maybe get the duration.
#
# ---------------------------------------------------------------------------------

# Manual override for home directory (normally not needed)
HOME=/u/home/g/groeling

# Manual override of scratch directory
#SCRATCH=/u/scratch/f/groeling
SCRATCH=$HOME/tv2

# Script name
SCRIPT=`basename $0`

# Help screen
if [ "$1" = "-h" -o "$1" = "--help" -o "$1" = "help" ]
 then echo -e "\n\t$SCRIPT\n"
  echo -e "\t\tDownload files from the digitizing lab and create .len markers for files to be processed.\n"
  echo -e "\tSyntax:\n"
  echo -e "\t\t$SCRIPT [<wait queue size>]\n"
 #echo -e "\t\t$SCRIPT <start date> <end date> [<wait queue size>]\n"
 #echo -e "\tYou can use the full date or a number of days ago.\n"
  echo -e "\tThe default wait queue size is 35 files.\n"
 #echo -e "\tExamples (default end date is today, but you can also go backwards):\n"
 #echo -e "\t\t$SCRIPT 3 (last three days, useful for cron)\n"
 #echo -e "\t\t$SCRIPT 2015-11-28 2015-11-30 5\n"
  echo -e "\tYou can interrupt the script when paused in the left margin.\n"
    exit
fi

# Wait queue size
if [ -z "$1" ]
  then Q="35"
  else Q="$1" #Q=$[Q-1]
fi

# Start date
if [ -z "$2" ]
 #then echo -e "\t\tUsage: $SCRIPT <start date> <end date>\n" ; exit
  then START=1970-01-01
  else START="$2"
fi

# Convert to a date if needed
if [ "$(echo "$START" | egrep '^[0-9]+$' )" ]
  then START="$(date -d "-$START day" +%F)"
fi

# End date
if [ -z "$3" ]
  then STOP="$( date +%F )"
  else STOP="$3"
fi

# Convert to a date if needed
if [ "$(echo "$STOP" | egrep '^[0-9]+$' )" ]
  then STOP="$(date -d "-$STOP day" +%F)"
fi

# Host
HOST="$( hostname -s )"

# Define the working directories
POND=$SCRATCH/pond
OCR=$SCRATCH/day
LOGS=$SCRATCH/logs

# Starting date
DAY=$START

# Welcome
echo -e "\n\t\tCreating an inventory of available video files\n"

# Move completed files into the appropriate directories (remote trigger not yet implemented on wd1 and wd2)
echo "" ; for RHOST in wd4 wd3 ; do ssh $RHOST m ; done ; echo ""

# Today's date for lists and log files
TAY=$( date +%F )

# Removed files log
RLOG=$LOGS/$TAY-removed.log

# Remove reservations from expired nodes -- maybe when you find one you delete all reservations for that file?
for RES in $POND/*d ; do if [ ! -e $RES ] ; then break ; fi
  JobID="$( cat $RES/* 2> /dev/null | sed -r 's/.*\t([0-9]{2,9})\t.*/\1/' )"
  if [ ! "$( qstat -j "$JobID" 2>1 | head -n1 )" ] ; then echo -e "\tRemoved expired node ${RES##*/}" | tee -a $RLOG ; rm -r $RES ; continue ; fi

  # Display candidate expired reservations (experimental -- develop logic)
  if [[ -d ${RES%.*}.reserved && ( ! -d ${RES%.*}.fetched || -d {RES%.*}.compressed ) ]] ; then
    AGE=0 ; while [ "$AGE" -eq 0 ] ; do AGE="$( date -r $RFIL.reserved +%s )" ; done
    if [ "$[ $AGE+$[100*60]]" -lt "$(date +%s)" ] ; then
      # If there's also no file inside the reservation directory
      if [ ! "$(ls -A $RES)" ]
        then echo -e "\tRemoved old and empty reservation $RES" | tee -a $RLOG ; rm -r $RES ; continue
        else echo -e "\tPossible expired reservation ${RES%.*}.reserved/`ls -1 $RES/*`"
      fi
    fi
  fi

  # If there's no file inside the reservation directory
  if [ ! "$(ls -A $RES)" ]; then echo -e "\tEmpty reservation $RES" ; fi

done

# Loop of remote systems (add to list) -- prefer the shuffle for good sampling, activate balancing when needed
#for RHOST in wd3 ; do
for RHOST in wd4 wd3 wd2 wd1 ; do
#for RHOST in `shuf -n1 -e wd4 wd3 wd2 wd1` ; do

  # Check each host
  #while true ; do

    # Get the number of len files from each source
    len1="$( cat $POND/*len 2>/dev/null | grep -c wd1 )"
    len2="$( cat $POND/*len 2>/dev/null | grep -c wd2 )"
    len3="$( cat $POND/*len 2>/dev/null | grep -c wd3 )"
    len4="$( cat $POND/*len 2>/dev/null | grep -c wd4 )"

    # Get more len files from the least represented source, softly privileging wd3 over wd2 over wd1
    #if [ "$len1" -lt "$len3" ] ; then RHOST=wd1 ; else RHOST=wd3 ; fi ; if [ "$len2" -lt "$len3" ] ; then RHOST=wd2 ; fi

    # Other possible criteria
    #for i in `ls -1d $POND/* | cut -d"." -f1 | uniq -u` ; do cat $POND/$i.len ; done
    #RHOST=`shuf -n1 -e wd2 wd1`

    echo -e "\n\t~ `date '+%F %H:%M'` ~ ${RHOST^^} ~  WD1 $len1 ~ WD2 $len2 ~ WD3 $len3 ~ WD4 $len4 ~"

    # Today's date for lists and log files
    TAY=$( date +%F )

    # Define a log for each day
    CLOG=$SCRATCH/completed/"$TAY"_fetch-len.log

    # List of texts
    TXT=/tmp/$TAY-fetch-$RHOST-txt-$$

    # List of videos
    MPG=/tmp/$TAY-fetch-$RHOST-mpg-$$

    # Count the .len files, but subtract the files that have already downloaded and are compressing
    CountLen="$( find $POND/ -maxdepth 1 -regextype sed -regex '.*_[A-Za-z0-9]\{7,8\}_.*len' 2>/dev/null | wc -l )"
    CountFch="$( find $POND/ -maxdepth 1 -regextype sed -regex '.*_[A-Za-z0-9]\{7,8\}_.*fetched' 2>/dev/null | wc -l )"
    CountRep="$( find $POND/ -maxdepth 1 -regextype sed -regex '.*_[A-Za-z0-9]\{7,8\}_.*repaired' 2>/dev/null | wc -l )"
    CountCmp="$( find $POND/ -maxdepth 1 -regextype sed -regex '.*_[A-Za-z0-9]\{7,8\}_.*compressed' 2>/dev/null | wc -l )"

    #CountLen="$( find $POND/ -maxdepth 1 -type f -name "*_Archive_*.len" 2>/dev/null | wc -l )"
    #CountFch="$( find $POND/ -maxdepth 1 -type d -name "*_Archive_*.fetched" 2>/dev/null | wc -l )"
    #CountRep="$( find $POND/ -maxdepth 1 -type d -name "*_Archive_*.repaired" 2>/dev/null | wc -l )"
    #CountCmp="$( find $POND/ -maxdepth 1 -type d -name "*_Archive_*.compressed" 2>/dev/null | wc -l )"
    COUNT=$[ CountLen - $[ CountRep + Countccx + CountCmp ] ]

    # Hold off if the queue is full
    if [ "$COUNT" -gt "$Q" ] ; then echo -n "." ; sleep 59.99 ; continue ; fi

    # If the cue is oddly low, try a random source
    #if [ "$COUNT" -lt "$[Q/2]" ] ; then RHOST=`shuf -n1 -e wd4 wd3 wd2 wd1`
    #  echo -e "\n\t~ `date '+%F %H:%M'` ~ ${RHOST^^} ~  WD1 $len1 ~ WD2 $len2 ~ WD3 $len3 ~ WD4 $len4 ~ (suspiciously low inventory)\n"
    #fi

    # Get a list of remote txt or cuts files that are older than two hours
    #ssh $RHOST "find /mnt/HD/HD_a2/Comm/VHS/ -maxdepth 1 -mtime +0 -type f -name '[1-2]*_0000_US_Archive_V*.txt' | sort -r" > $TXT
    ssh $RHOST "find /mnt/HD/HD_a2/Comm/VHS/ -maxdepth 1 -mmin +120 -regextype sed -regex '.*\/[0-9\-]\{10\}_0000_US_[A-Za-z0-9]\{7,8\}_V.*[A-Z]\{2\}\(_2\)\?\..*txt' | sort -r" > $TXT

    #cat $TXT ; echo ""

    # Get a list of remote mpg files that are older than two hours
    #ssh $RHOST "find /mnt/HD/HD_a2/Comm/VHS/ -maxdepth 1 -mtime +0 -type f -name '[1-2]*_0000_US_Archive_V*.mpg' | sort -r" > $MPG
    #ssh $RHOST "find /mnt/HD/HD_a2/Comm/VHS/ -maxdepth 1 -mmin +120 -regextype sed -regex '.*\/[0-9\-]\{10\}_0000_US_Archive_V.*[A-Z]\{2\}\.mpg' | sort -r" > $MPG
    ssh $RHOST "find /mnt/HD/HD_a2/Comm/VHS/ -maxdepth 1 -mmin +120 -regextype sed -regex '.*\/[0-9\-]\{10\}_0000_US_[A-Za-z0-9]\{7,8\}_V.*[A-Z]\{2\}\(_2\)\?\.mpg' | sort -r" > $MPG

    # cat $MPG ; echo ""

    # Verify the mpg exits -- don't match on the last initials
    for i in `cat $TXT` ; do grep ${i%_*} $MPG ; done | sponge $TXT

    # Clean up the list
    for i in `cat $TXT` ; do i=${i##*/} ; echo ${i%%.*} ; done | sort -u | sort -r > /tmp/$TAY-fetch-$RHOST-list-$$

    # cat $TXT ; echo "" ; exit

    # if too few cutpoint files are ready
    if [ "$( cat $TXT | wc -l )" -lt "$Q" ] ; then

      # Clean up the list and add the mpg files to the end (the files that have cutpoint files will be harmlessly duplicated)
      for i in `cat $MPG` ; do i=${i##*/} ; echo ${i%%.*} ; done | sort -u | sort -r >> /tmp/$TAY-fetch-$RHOST-list-$$

    fi

    # Debug
    #cat /tmp/$TAY-fetch-$RHOST-list-$$ ; exit

    # Initialize counters
    # m is n/p, used to identify a period within the loop -- during the period, you skip counting files because it slows us down too much (dynamic)
    # n is the ordinal number of loop passes (and thus files in the current list from cartago) (start value 0)
    # o says how short the wait queue must be to start creating loop periods (normal value 30, fixed)
    # p is the length of the loop period -- the number of files within one period (normal start value 1, dynamic)
    n=0 o=$[Q-5] p=1

    # List the files to generate a cleaned-up master list
    for FIL in `cat /tmp/$TAY-fetch-$RHOST-list-$$` ; do

      # Peel off path and extension
      FIL=${FIL##*/} FIL=${FIL%.*}

      # Tree
      DDIR="$(echo $FIL | sed -r 's/([0-9]{4})-([0-9]{2})-([0-9]{2}).*/\1\/\1-\2\/\1-\2-\3/')" ; mkdir -p $OCR/$DDIR

      # Shorthand file names
      F=$POND/$FIL OFIL=$OCR/$DDIR/$FIL

      # Debug
      echo -e "\r\t$( date +%Y-%m-%d\ %H:%M:%S ) \tCheck\t$FIL"

      # Skip files with hyphens
      if [ "$( echo ${FIL#*_} | grep -o '-' )" ] ; then continue ; fi

      # Skip files before beginning or after end of analog recording
      if [ ${FIL:0:4} -lt 1972 -o ${FIL:0:4} -gt 2006 ] ; then continue ; fi

      # Skip if the file has been processed
      if [[ -f $OFIL.txt3 && -d $OFIL.mp4.done ]] ; then continue ; fi

      # Skip if the file is already listed or is being converted
      if [[ -f $F.len || -d $F.fetched ]]
        then continue ; else echo -n "" ; n=$[n+1]
      fi

      # Counting slows us down, so skip some if you're falling behind
      m=$( echo "scale=2; $n/$p" | bc )

      # Debug
      #echo -e "\r\t$( date +%Y-%m-%d\ %H:%M:%S ) \t\tm is $m n is $n o is $o p is $p Q is $Q and FIL is $FIL"

      # Count the queued files every pth time (a count can take a couple of minutes or more on a crowded day)
      if [ $n = 1 -o ${m#*.} = 00 ] ; then echo -n .
	# Count the .len files, but subtract the files that have already downloaded and are compressing
	CountLen="$( find $POND/ -maxdepth 1 -regextype sed -regex '.*_[A-Za-z0-9]\{7,8\}_.*len' 2>/dev/null | wc -l )"
	CountFch="$( find $POND/ -maxdepth 1 -regextype sed -regex '.*_[A-Za-z0-9]\{7,8\}_.*fetched' 2>/dev/null | wc -l )"
	CountRep="$( find $POND/ -maxdepth 1 -regextype sed -regex '.*_[A-Za-z0-9]\{7,8\}_.*repaired' 2>/dev/null | wc -l )"
	CountCmp="$( find $POND/ -maxdepth 1 -regextype sed -regex '.*_[A-Za-z0-9]\{7,8\}_.*compressed' 2>/dev/null | wc -l )"
	COUNT=$[ CountLen - $[ CountRep + Countccx + CountCmp ] ]
      fi

      # Skip some counts if the queue is less than $o
      #if [ "$COUNT" -lt "$o" ] ; then p=5 ; else p=1 ; fi

      # Skip counting more often if the queue is less than half $o
      #if [ "$COUNT" -lt "$[o/2]" ] ; then p=$Q ; fi

      # Hold off if the queue is full
      while [ "$COUNT" -gt "$Q" ] ; do
        #echo -en "\r\t$( date +%Y-%m-%d\ %H:%M:%S ) \t   $COUNT \tQueue full" ; sleep 67.45
        echo -e "\r\t$( date +%Y-%m-%d\ %H:%M:%S ) \t   $COUNT \tQueue full\n" ; sleep 67.45 ; break 2
      done

      # If you have time, check again to see if the file is already listed or has already been converted (takes up to ten seconds)
      if [ "$COUNT" -gt "$o" ] ; then #echo -e "\r\t$( date +%Y-%m-%d\ %H:%M:%S ) \tCheck\t$FIL"
        if [[ -f $F.len || -d $F.fetched || -d $F.compressed || -d $F.mp4.done || -f $OFIL.txt3 || -d $F.framed || -f $OFIL.ocr ]]
          then continue ; else echo -n ""
        fi
      fi

      # Get the cutpoints file if available (may have extension .txt or .cuts.txt -- move to .cuts)
      CUTS="$( ssh $RHOST "ls -1 /mnt/HD/HD_a2/Comm/VHS/${FIL%_*}*.txt 2>/dev/null" )"
      if [ "$CUTS" != "" ] ; then CUTS=${CUTS##*/} CUT="Cutpoints"
        rsync $RHOST:/mnt/HD/HD_a2/Comm/VHS/$CUTS $OCR/$DDIR/${CUTS%%.*}.cuts -aq --chmod=Fu=rw,Fg=r,Fo=r 2>&1 >/dev/null
        else CUT="No cuts"
      fi

      # Copy the cutpoints file to storage
      if [ -f $OCR/$DDIR/${CUTS%%.*}.cuts ] ; then rsync $OCR/$DDIR/${CUTS%%.*}.cuts ca:/mnt/ifs/NewsScape/Rosenthal/$DDIR/ -aq &>/dev/null ; fi

      # Generate a .len file (get video duration if available)
      #DUR="$( grep 'DUR|' $OCR/$DDIR/$FIL.txt | cut -d"|" -f2 )" ffDURs="$(date -ud 1970-01-01\ $DUR +%s)"
      #if [ "$ffDURs" -gt 10 ] ; then echo "$RHOST 29700 29700 0 $ffDURs $ffDURs 0 $FIL.mpg" > $POND/$FIL.len ; fi
      echo "$RHOST 0 0 0 29700 29700 0 $FIL.mpg" | tee $POND/$FIL.len >$OFIL.len

      # Submit a job request
      #QNUM="$( qsub ~/bin/all-4c-24l.cmd | grep submitted | cut -d" " -f3 )"

      # Receipt
      echo -e "\t$( date +%Y-%m-%d\ %H:%M:%S ) \t ${RHOST^^} \t$CUT $COUNT \t$FIL.len" >> $CLOG ; echo ""
      echo -e "`date +%F\ %H:%M:%S` \tfetch-len \t$(hostname -s) \t${RHOST^^} \t$CUT \t$COUNT downloads  \t$FIL.len" | tee -a $LOGS/reservations.$( date +%F )

    done

    # Clean up
    # cat /tmp/$TAY-fetch-$RHOST-list-$$
    rm -f $TXT $MPG /tmp/$TAY-fetch-$RHOST-list-$$

    # Pause between source systems
    #sleep 300

    # Next day (not working properly going backwards -- fixme!)
    #if [ "$( date -d "$START" +%s )" -gt "$( date -d "$STOP" +%s )" ]
    #  then DAY=$( date -d "-1 day"\ $DAY +%F )
    #    if [ "$( date -d "-1 day"\ $STOP +%F )" = "$DAY" ] ; then echo -e "\n\tExiting after $STOP as requested.\n" | tee -a $CLOG ; exit ; fi
    #  else DAY=$( date -d "+1 day"\ $DAY +%F )
    #    if [ "$( date -d "+1 day"\ $STOP +%F )" = "$DAY" ] ; then echo -e "\n\tExiting after $STOP as requested.\n" | tee -a $CLOG ; exit ; fi
    #fi ; read -t 10.02 -n 1 -s

    # break

  #done # End of day loop

done # End of remote system loop

# Save the fetch log
if [ -f $CLOG ] ; then scp -p $CLOG ca:/mnt/ifs/NewsScape/Rosenthal/logs &>/dev/null ; fi
#if [ -f $LOGS ] ; then scp -p $LOGS ca:/mnt/ifs/NewsScape/Rosenthal/logs ; fi

# Clean up
rm -f /tmp/$TAY-fetch-$RHOST-list-$$

# EOF
