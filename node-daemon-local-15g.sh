#!/bin/bash
#
# /u/home/g/groeling/bin/node-daemon-local.sh
#
# Occupies a node and calls each processing component in turn
#
# Document in http://vrnewsscape.ucla.edu/cm/Hoffman2
#
# Written on 25 October 2013, FFS
#
# Overview of new task pipeline for incoming recordings (mostly completed and working)
#
#	* copy2hoffman on the recording nodes copies $FIL.len to the ~/tv/pond)
#       * work-daemon-local.sh reserves some grid engine jobs for node-daemon-local, using all-4c-24l.cmd
#	* node-daemon-local.sh looks for .len files, reserves a file, calls fetch2node.sh, and starts the processing scripts
#	* the processing scripts frames.sh, repair.sh, ts2mp4.sh, and jpg2ocr.sh have been modified to work with local files
#	* copy2csa.sh copies the .hq, .ocr, and .mp4 files to storage, through a forwarded port on one of the login nodes
#	* downside: files are dependent on a single grid engine job, though cleanup.sh can be changed to restore reservations (needs to be done)
#	* downside to this new pipeline: jobs are sequential, copying is not robust, monitoring and cleanup not written
#	* summary: this new pipeline is potentially more robust if $SCRATCH is really slow, otherwise it's not as good as the shared solution
#
# To do:
#
#	2012-12-31: fix remote log
#
# Changelog
#
#	2016-04-15 Blacklist 2211
#	2016-03-07 Remove expired reservations -- adjusted from 10 to 1000 minutes on 2016-04-04
#	2016-02-28 Start a new and terminate current job if file transfer fails
#	2016-02-16 Groeling fork of node-daemon-local-25 for Archive files -- node-daemon-local-00g
#	2016-02-14 Limit Archive files to one
#	2016-02-10 Move cc-extract-la.sh to textract.name
#	2015-12-01 Do not background processes -- you'll use too much CPU
#	2015-09-21 Upped MAXDROP from 10 to 60 -- small drops are usually unavoidable on both versions of a file
#	2014-07-28 Upped minimum time required from video length times 2 to 2.5
#	2014-05-26 Add text extraction, start using node-daemon-local.name
#	2014-04-17 Change order so repair runs before frames
#	2013-11-25 Forked from ocr-daemon-local.sh
#	2013-11-16 Move deletion of old files to cleanup.sh
#	2013-11-07 On our own nodes, pause when busy with incoming
#	2013-11-06 Stop runaway reservations at 5
#	2013-11-04 Silent lock
#       2013-10-25 Forked from node-daemon.sh -- local files, frames and ocr
#	2013-03-09 ${CHILD%.*}.name option to allow live script updates
#	2013-03-08 separate logs
#       2013-03-01 forked from work-daemon.sh from 2008
#
# WARNING: Any changes you make to this script will affect currently running processes!
# Maintain a succession of numbered versions, node-daemon-local-00.sh to 99, and activate the last one
# using echo node-daemon-local-04.sh > node-daemon-local.name -- used by all-4c-24l.cmd and all-hp-14d.cmd
#
# --------------------------------------------------------------------------------------------------------

SCRIPT=`basename $0`

# Help screen
if [ "$1" = "-h" -o "$1" = "--help" -o "$1" = "help" ]
 then echo -e "\n\t$SCRIPT\n"
  echo -e "\t\tThis node daemon uses local source files."
  echo -e "\t\tIt copies incoming files from the recoring machines"
  echo -e "\t\tand repairs, extracts frames, compresses, and decodes on-screen text.\n"
  echo -e "\tSyntax:\n"
  echo -e "\t\t$SCRIPT [<max drop>] [<threads>] [<hours>]\"&\n"
  echo -e "\tDefault max drop tolerated is 10 seconds"
  echo -e "\tDefault threads is 4, legal values 1-8"
  echo -e "\tHours requested is typically 24 -- slack is adjusted by video duration.\n"
  echo -e "\tRun job.q or openmp.q to create cmd files for different scripts."
  echo -e "\tTweak and submit the cmd files using qsub.\n"
  echo -e "\tCreate $HOME/stop-$SCRIPT.\$QNUM for soft exit.\n"
   exit
fi

# Tolerate up to max seconds of drops
if [ -z "$1" ]
  then MAXDROP="60"
  else MAXDROP="$1"
fi

# Optional limit on the number of cores
if [ -z "$2" ]
  then THREADS="4"
  else THREADS="$2"
fi

# Number of hours requested
if [ -z "$3" ]
  then HOURS="24"
  else HOURS="$3"
fi

# Host name
HOST="$( hostname -s )"

# Start time
START=$(date +%s)

# Initialize the fast pond-level tracker (FIXME -- how do we maintain the pond-level tracker in occupy mode?)
echo -e "\t$( date +%a\ %b\ %d\ %T\ %Y )\t0\t$CUR" > $HOME/tv2/pond-levels

# Home directory
HOME=/u/home/g/groeling

# Local executables
LBIN=$HOME/bin

# Scratch directory
#SCRATCH=/u/scratch/f/groeling

# Primary reservations
RDIR=$HOME/tv2/pond

# Log directory
LOGS=$HOME/tv2/logs

# Local storage
SDIR=/work/pond ; mkdir -p /work/pond

# Work directory
WORK=/work/day ; mkdir -p /work/day

# Initialize log count
NUM=0 TARGET=0

# Get the current grid engine queue number
QNUM="$( echo $PATH | sed -r 's/.*\/work\/([0-9]{3,7})\..*/\1/' )"

# On exclusive nodes, use the process list
if ! [[ "$QNUM" =~ ^[0-9]+$ ]] ; then QNUM="$( ps x | grep job_scripts | grep -v grep | tail -n 1 | sed -r 's/.*\/([0-9]{2,8})/\1/' )" ; fi

# On interactive nodes, infer the queue number from the node name
if ! [[ "$QNUM" =~ ^[0-9]+$ ]] ; then QNUM="$( myjobs | grep `hostname -s` | egrep "QRLOGIN|$SCRIPJ" | tail -n 1 | cut -d" " -f1 )" ; fi

# If the queue number is not found, use zero
if ! [[ "$QNUM" =~ ^[0-9]+$ ]] ; then QNUM=00000 ; fi

# Blacklist
if [ "$HOST" = "n2211" ] ; then qdel $QNUM ; exit ; fi

# End time (adjusted below by video duration)
TIMEOUT=$[$(date +%s) + $[3600 * $HOURS]]

# Debug
echo -e "\n\tStarts at $(date +%s) ~ $(date +%Y-%m-%d\ %H:%M:%S)\n"
echo -e "\n\tEnds at   $TIMEOUT ~ $(date -d @$TIMEOUT +%Y-%m-%d\ %H:%M:%S) ~ $SCRIPT ~ $HOST ~ \n"

# Deamon loop
while true ; do

  # Initialize
  ffDURs="0" QLEN="0"

  # Just hang around if there's a file with extension wait
  if [ -f /work/pond/*.wait ] ; then sleep 66.67 ; continue ; fi

  # Check for waiting incoming files
  #for FIL in $( ls -1d $HOME/tv2/pond/*.{len,mpg,reserved} 2>/dev/null | xargs -n 1 basename | cut -d"." -f1 | uniq -u ) ; do
  for FIL in $( ls -1d $HOME/tv2/pond/*.len 2>/dev/null | xargs -n 1 basename | cut -d"." -f1 | uniq -u ) ; do

    # Path
    DDIR="$(echo $FIL | sed -r 's/([0-9]{4})-([0-9]{2})-([0-9]{2}).*/\1\/\1-\2\/\1-\2-\3/')"

    # Short names
    RFIL=$HOME/tv2/pond/$FIL SFIL=/work/pond/$FIL

    # Verify you have a .len
    if [ ! -e $RFIL.len ] ; then continue ; fi

    # Skip reserved files
    if [[ -d $SFIL.reserved || -d $RFIL.reserved || -d $RFIL.repaired || -d $RFIL.OCRed || -d $RFIL.framed ]] ; then continue ; fi

    # Set the reservations log
    LLOG=$LOGS/reservations.$( date +%F ) ; touch $LLOG

    # Stop runaway reservations (experimental)
    RESD="$( ls -1d /work/pond/*.reserved/$QNUM 2>/dev/null | wc -l )"
    if [ "$RESD" -gt "5" ] ; then
      echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT:0:11} \t$QNUM\t$HOST \tRUNAWAY    \t$RESD jobs        \t$FIL" | tee -a $LLOG ; sleep 60.01 ; continue
    fi

    # On our own nodes, pause if the node is already processing some rival number of files
    #if [ "$HOST" = "n6288" -o "$HOST" = "n7288" ] ; then RIVAL=4
    #  if [ "$( ps x | grep bash | grep -v grep | egrep -c '[0-9-]{10}' )" -ge "$RIVAL" ] ; then pause 600 ; fi
    #fi

    # Get the host name and show duration from the .len file
    read RHOST CCWORDS CCWORDS2 CCDIFF ScheDUR ffDURs TIMDIFF LENFIL <<< $( head -n 1 $RFIL.len )
    #LEN="$(date -ud "+$ffDURs seconds"\ $(date +%F) +%H:%M:%S)"

    # If you have a video duration, multiply by some number to get the job length (calibrate as needed); otherwise skip (QLEN multiplier is 2)
    if [ -z "$ffDURs" -o "$ffDURs" = "0" ] ; then continue ; else QLEN="$( printf %0.f "$( echo "scale = 3; ($ffDURs * 2)" | bc )" )" ; fi

    # Start of job in unix epoch
    JobStart="$( date -d "`qstat -j $QNUM | grep 'submission_time' | cut -d" " -f2-99`" +%s )"

    # End of job in unix epoch
    JobEnd="$[ $JobStart + $( qstat -j $QNUM | egrep -o 'h_rt=[0-9]{1,9}' | cut -d"=" -f2 ) ]"

    # Estimated time required to process the current video
    TimeNeeded=$[ $( date +%s ) + $QLEN ]

set -xv
    # Ensure there is enough time left on this node to process a video of this length
    if [ $TimeNeeded -gt $JobEnd ] ; then rm -r $SFIL.* $RFIL.*d ; sleep 120
      echo -e "\n\t$FIL timed out at $(date +%Y-%m-%d\ %H:%M:%S) for job ending $(date -d @$JobEnd +%Y-%m-%d\ %H:%M:%S)\n" ; qdel $QNUM
    fi
set +xv

    # Soft exit
    if [ -f $HOME/stop-$SCRIPT.$QNUM ] ; then echo -e "\n\tSoft exit $HOME/stop-$SCRIPT.$QNUM\n" ; exit ; fi

    # Skip reserved files
    if [[ -d $RFIL.reserved || -d $RFIL.repaired || -d $RFIL.OCRed || -d $RFIL.framed ]] ; then continue ; fi

    # Attempt to reserve the file -- primary reservation
    if [ "$( mkdir $RFIL.reserved 2> /dev/null; echo $? )" = "0" ]
      then echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT:0:11} \t$QNUM\t$HOST \tLocking \t$RHOST \t$FIL" > $RFIL.reserved/$QNUM
      else echo -e "\n\t$FIL is already reserved for full processing.\n" ; continue
    fi

    # Secondary local reservation
    if [ "$( mkdir $SFIL.reserved 2> /dev/null; echo $? )" = "0" ]
      then echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT:0:11} \t$QNUM\t$HOST \tLocking \t$RHOST \t$FIL" > $SFIL.reserved/$QNUM
      else echo -e "\n\t$FIL is already reserved for full processing.\n" ; continue
    fi

    # Verify
    if [ ! -d "$RFIL.reserved" ] ; then echo -e "\tUnable to reserve a file from the queue on ~/tv/pond" ; continue ; fi

    # Use the latest fetch2node script
    if [ -f $LBIN/fetch2node.name ] ; then FETCH="$( cat $LBIN/fetch2node.name )" ; else FETCH=$LBIN/fetch2node.sh ; fi

    # Check that the script exists
    if [ ! -f $LBIN/$FETCH ] ; then echo -e "\nUnable to find the script $LBIN/$FETCH.\n" ; exit ; fi

    # Fetch the file
    if [ ! -f "$SFIL.mpg" ] ; then $LBIN/$FETCH $RHOST $FIL.mpg ; fi

set -xv
    # Verify and clean up if failure -- wait a few seconds to make sure the cluster file system shows the reservations
    if [ -f $SFIL.mpg ]
      then echo -e "\n\tProcessing $SFIL.mpg ...\n"
      else
        #QNUM2="$( qsub $HOME/bin/all-4c-24l.cmd | grep submitted | cut -d" " -f3 )"
        #echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT:0:11} \t$QNUM\t$HOST \tFAILED \t$QNUM2 started \t$FIL" | tee -a $LLOG
        ls -l $RFIL.*d $SFIL.*d
        sleep 12.02 ; rm -rf $RFIL.*d $SFIL.*d ; sleep 12.02 ; rm -rf $RFIL.*d $SFIL.*d ; continue
    fi
set +xv

    # Ensure the file is non-zero
    if [ ! -s $SFIL.mpg ] ; then sleep 12.03 ; rm -rf $SFIL.* $RFIL.*d ; continue ; fi

    # Skip completed files
    #if [ -f $RFIL.done ] ; then rm -rf $SFIL.* $RFIL.reserved ; continue ; fi

    # Run the scripts in succession
    #for CHILD in repair.sh textract.sh frames.sh ts2mp4.sh jpg2ocr.sh ; do
    for CHILD in repair.sh textract.sh ts2mp4.sh ; do

      # To permit live script updates, retrieve the appropriate script version from the corresponding .name file
      # For instance, ~/bin/jpg2ocr.name keeps the value jpg2ocr-4.sh (use this instead of symlinks)
      if [ -f $LBIN/${CHILD%.*}.name ] ; then CHILD="$( cat $LBIN/${CHILD%.*}.name )" ; fi

      # Check that the script exists
      if [ ! -f $LBIN/$CHILD ] ; then echo -e "\nUnable to find the script $LBIN/$CHILD.\n" ; exit ; fi

      # Testing
      #if [[ $CHILD == jpg2ocr* ]] ; then CHILD=jpg2ocr-0local.sh ; fi

      # Stem
      NWK=${FIL:19:30} ; STEM=${FIL:0:19}${NWK%%_*}

      # Repair the file
      if [[ $CHILD == repair* ]] ; then

        # Verify
        if [ ! -f $SFIL.mpg -o -f $SFIL.ts ] ; then continue ; fi

        # Repair the file
        echo -e "\tStarting $CHILD $FIL.mpg $MAXDROP $THREADS local on $HOST at $( date +%Y-%m-%d\ %H:%M:%S )\n"

	# This is not yet working, and may have degraded OCR 2014-06-03 to 13 with dummy txt files

        # Start the requested job and log
        #$LBIN/$CHILD $FIL $MAXDROP $THREADS local >& $LOGS/$CHILD-$STEM.$QNUM
        $LBIN/$CHILD $SFIL $MAXDROP $THREADS local

        # Debug
        echo -e "\n\tAfter repair:" ; date ; pwd ; ls -ld $SFIL.*

        continue

      fi

      # Extract metadata
      if [[ $CHILD == textract* ]] ; then

        # Extract text (do not include path)
        $LBIN/$CHILD $FIL

        # Debug
        echo -e "\n\tAfter text extraction:" ; date ; pwd ; ls -ld /work/day/$DDIR/$FIL.*

        continue

      fi

      # Frame extract script skips hq files or its own reservations
      if [[ $CHILD == frames* ]] ; then

        if [[ -d $SFIL.hq || -d $SFIL.framed || -f /work/pond/$FIL.ocr ]] ; then sleep 0.1 ; continue ; fi

        # Start the requested job and log -- do not background it
        $LBIN/$CHILD $SFIL $MAXDROP $THREADS local >& $LOGS/$CHILD-$STEM.$QNUM

	# Continue if you succeed -- but what do you do if you don't?
        if [[ -d $SFIL.hq || -d $SFIL.framed || -f /work/pond/$FIL.ocr ]] ; then sleep 0.1 ; continue ; fi

      fi

      # Before compression wait two minutes
      if [[ $CHILD == ts2mp4* ]] ; then sleep 12 ; fi

      # OCR script checks for hq
      if [[ $CHILD == jpg2ocr* ]] ; then continue ; sleep 70
        if [[ $CHILD == jpg2ocr* && ! -d $SFIL.hq ]] ; then ls -ld $FIL.*
          echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT:0:11} \t$QNUM\t$HOST \tNo images \t$ffDURs \t\t$FIL" | tee -a $LLOG ; continue
        fi
      fi

      # On exclusive nodes, pause if you're close to the target number of jobs (load balancing gesture)
      #if [ "$( ps x | grep pond | grep $CHILD | grep -v grep | wc -l )" -ge "$[TARGET-2]" ] ; then sleep 66.666 ; fi

      # Debug
      echo -e "\tStarting $CHILD $FIL.mpg $MAXDROP $THREADS local on $HOST at $( date +%Y-%m-%d\ %H:%M:%S )\n"

      # Stem
      NWK=${FIL:19:30} ; STEM=${FIL:0:19}${NWK%%_*}

      # Start the requested job and log
      $LBIN/$CHILD $SFIL $MAXDROP $THREADS local >& $LOGS/$CHILD-$STEM.$QNUM

      sleep 35

    done  # Complete the loop of scripts to run on each len file

    echo -e "\n\tThis is a good time to interrupt this script! You have 68.9009 seconds.\n" ; sleep 68.9009

  done    # Complete the loop of .len files in the central pond

  # Debug
  echo -e "\n\tLoop at $(date +%s) ~ $(date +%Y-%m-%d\ %H:%M:%S) ~ $SCRIPT ~ $HOST ~ \n"
  echo -e "\n\tEnds at $TIMEOUT ~ $(date -d @$TIMEOUT +%Y-%m-%d\ %H:%M:%S) ~ $SCRIPT ~ $HOST ~ \n"

  # Check interval
  if [ "$( ls -ctr1 $HOME/tv2/pond/*.len 2>/dev/null )" = "" ] ; then sleep 55.99 ; else sleep 33.99 ; fi

done      # Complete the loop of jobs to run continually on an occupied node until time runs out

# EOF
