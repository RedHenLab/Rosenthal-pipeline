#!/bin/bash
#
# ~/bin/repair.sh
#
# Repair incoming mpg files to ts
#
# Called by ~/bin/node-daemon.sh
#
# Written 2012-12-20 FFS
#
# Document in http://vrnewsscape.ucla.edu/cm/Hoffman2
#
# Changelog:
#
#	2016-02-16 Groeling fork of repair-25.sh for Archive files -- repair-00g.sh
#	2016-02-10 Always run dvbt-ts-repair again if project-x worked -- and reject the file if it fails
#       2015-12-17 Improve video duration detection
#	2014-05-29 Try hoffman2.idre.ucla.edu first
#	2014-04-20 Capture output of dvbt-ts-repair for the log
#	2014-04-17 Try dvbt-ts-repair before project-x
#       2014-01-25 Include ~/tv/pond as potential location for half-processed files (from an interrupted process)
#       2013-12-03 Use $HOME/tv2 instead of $SCRATCH, add $HOST to logs
#	2013-11-29 Reduced sleep cycle from ten to one minute -- not sure why it was set so high
#       2013-10-15 Add parameter for scratch directory to use local drive
#	2013-09-11 Add local host name
#	2013-04-29 Double max processing time to allow for a very slow scratch space
#	2013-02-28 Separate out projectx
#	2013-02-10 Switch projectx from -tom2p to -tots to preserve metadata information
#	2013-01-28 Add max drop user variable
#	2013-01-25 Add duration check on repaired video
#	2013-01-10 Added a soft kill for projectx -- retry once if killed for excessive processing time
#	2013-01-06 Use `basename $0` to permit switching between versions
#	2012-12-29 Repair all files with projectx -- simplifies workflow and reduces stalls
#
# Todo:
#
#	2012-12-23 better logging
#
# WARNING: Any changes you make to this script will affect currently running processes!
# Maintain a succession of numbered versions, repair-00.sh to 99, and activate the last one
# using echo repair-15.sh > repair.name -- used by node-daemon-local.
#
# ------------------------------------------------------------------------------------------

# Manual override for home directory
HOME=/u/home/g/groeling

# Manual override of scratch directory
#SCRATCH=/u/scratch/f/groeling
SCRATCH=$HOME/tv2

# Script name
SCRIPT=`basename $0`

# Dereference script name when called manually
SCRIPT="$( readlink -f $HOME/bin/$SCRIPT )" SCRIPT=${SCRIPT##*/}

# Help screen
if [ "$1" = "-h" -o "$1" = "--help" -o "$1" = "help" ]
 then echo -e "\n$SCRIPT"
  echo -e "\n\tThe repair script repairs and converts an mpeg2 file to ts\n"
  echo -e "\tSyntax:\n"
  echo -e "\t\t$SCRIPT <filename> [<max drop>] [<threads>] [local]\n"
  echo -e "\tThe script currently assumes ~/tv2/pond and extension mpg."
  echo -e "\tAccept up to maximum number of seconds dropped, default 10."
  echo -e "\tDefault threads is auto, legal values 1-8, but only one core is currently used."
  echo -e "\tOptionally set scratch to the local drive on a compute node.\n"
  echo -e "\tThe script is called by node-daemon-local.sh\n"
   exit
fi

# Start time
START=$( date +%s )

# Primary reservations
POND=$SCRATCH/pond

# OCR tree
OCR=$SCRATCH/day

# Shared or local processing (you should standardize the variable names here -- fixme)
if [ -z "$4" ]

  # Shared processing
  then WORK=$SCRATCH/pond

    # Backup reservations
    R2DIR=$HOME/reservations

  # Local processing -- local source but shared scratch
  else WORK=/work/pond ; mkdir -p $WORK

    # Alternative pond (from interrupted jobs)
    S2DIR=/u/scratch/f/groeling/pond

    # Backup reservations
    R2DIR=$WORK

fi

# Host
HOST=$( hostname -s )

# Get the file name
if [ -z "$1" ]
  then echo -e "\n\tPlease give a file name to process\n" ; exit
  else FIL="$1"
fi

# Strip path and extension -- assume $WORK and mpg
FIL="${FIL##*/}" FIL="${FIL%.*}" EXT="mpg" FFIL=$WORK/$FIL.$EXT

# Copy the files from the scratch spool if present -- used for interrupted jobs
#if [ ! -f $WORK/$FIL.$EXT ] ; then if [ -f $S2DIR/$FIL.$EXT -o -d $S2DIR/$FIL.hq ] ; then mv $S2DIR/$FIL.* $WORK/ ; fi ; fi

# Verify the file exists
if [ ! -f $WORK/$FIL.$EXT ] ; then echo -e "\n\tNot finding a file called $FIL.$EXT in the pond\n" ; exit ; fi

# Skip ts files
if [ -f $WORK/$FIL.ts ] ; then rm -rf $WORK/$FIL.repaired ; echo -e "\n\t$WORK/$FIL.ts exists.\n" ; exit ; fi

# Tolerate up to max number of seconds dropped
if [ -z "$2" ]
  then MAXDROP=10
  else MAXDROP=$2
fi

# Optional limit on the number of cores
if [ -z "$3" ]
  then THREADS="1"
  else THREADS="$3"
fi

# Get the age of the file -- for some reason this command occasionally fails, so persist
AGE=0 ; while [ "$AGE" -eq 0 ] ; do AGE="$( date -r $FFIL +%s )" ; done ; NOW="$(date +%s)" ; DIFF=$[NOW-AGE]

# Skip files that were updated less than a minute ago -- they may still be arriving
if [ "$DIFF" -lt "60" ] ; then echo -e "\n\t$FIL is less than a minute old\n" ; exit ; fi

# Holding pen for mp4 files with dropped video (partially failed conversions)
DROPS=$SCRATCH/drops

# Log directory
LOGS=$SCRATCH/logs

# Get the current grid engine queue number
QNUM="$( echo $PATH | sed -r 's/.*\/work\/([0-9]{3,7})\..*/\1/' )"

# On exclusive nodes, use the process list
if ! [[ "$QNUM" =~ ^[0-9]+$ ]] ; then QNUM="$( ps x | grep job_scripts | grep -v grep | tail -n 1 | sed -r 's/.*\/([0-9]{3,8})/\1/' )" ; fi

# On interactive nodes, infer the queue number from the node name
if ! [[ "$QNUM" =~ ^[0-9]+$ ]] ; then QNUM="$( myjobs | grep `hostname -s` | egrep "QRLOGIN|$SCRIPJ" | tail -n 1 | cut -d" " -f1 )" ; fi

# If the queue number is not found, use zero
if ! [[ "$QNUM" =~ ^[0-9]+$ ]] ; then QNUM=0000000 ; fi

# Log file stem
NWK=${FIL:19:30} ; STEM=${FIL:0:19}${NWK%%_*}

# Derive the log file name
QLOG=$LOGS/$SCRIPT-$STEM.$QNUM

# Load module support, perl module for dvbt-ts-repair, and java module for projectx
. /u/local/Modules/default/init/modules.sh
module load perl
module load java

# Perl libraries
PERL5LIB=/u/local/apps/perl_modules/lib64/perl5:/u/local/apps/perl_modules/lib/perl5:/u/local/apps/perl_modules/share/perl5

# Add libraries for local ffmpeg (ffprobe)
export LD_LIBRARY_PATH+=:$HOME/lib/

# Debug
echo -e "\n\tLD_LIBRARY_PATH line 106 is $LD_LIBRARY_PATH\n"

# Set the reservations log
LLOG=$LOGS/reservations.$( date +%F ) ; touch $LLOG

# Generate tree
DDIR="$(echo $FIL | sed -r 's/([0-9]{4})-([0-9]{2})-([0-9]{2}).*/\1\/\1-\2\/\1-\2-\3/')"

# Run the age test again -- it can fail even when a date value is obtained
AGE=0 ; while [ "$AGE" -eq 0 ] ; do AGE="$( date -r $FFIL +%s )" ; done ; NOW="$(date +%s)" ; DIFF=$[NOW-AGE]

# Skip files that were updated less than a minute ago -- they may still be arriving
if [ "$DIFF" -lt "61" ] ; then echo -e "\n\t$FIL is less than a minute old\n" ; exit ; fi

# Move on
DIFF="$(date -ud "+$DIFF seconds"\ $(date +%F) +%H:%M:%S)"

# Get the video length in hh:mm:ss.nn
DUR=$( $HOME/bin/ffprobe $WORK/$FIL.mpg 2>&1 | grep Duration | sed -rn s/'.*([0-9]{1}:[0-9]{1,2}:[0-9]{1,2}\.[0-9]{2,3}).*/\1/p' )

# Or just make it up?
if [ -z "$DUR" -o "$DUR" = "0" ] ; then DUR="08:12:00.00" ; fi

# Convert to seconds
LEN="$(date -ud 1970-01-01\ $DUR +%s)"

# If this still fails, exit -- several operations below depend on a good length value
if [ -z "$LEN" -o "$LEN" = "0" ]
  then echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \tFailing \tNo length \t$FIL.$EXT" | tee -a $LOGS/reservations.$( date +%F ) ; exit
fi

# Check the file is non-zero (may just be being sync'ed to storage and deleted)
if [ ! -s $FFIL ] ; then echo -e "\n\t$FIL is an empty file\n" ; exit ; fi

# Ensure that the file is reserved (the reservation may have been deleted by the node-daemon) -- you should check this belongs to the current node
if [ "$( mkdir $POND/$FIL.reserved 2> /dev/null; echo $? )" = "0" ]
  then echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT:0:11} \t$QNUM\t$HOST \tLocking \t$RHOST \t$FIL" > $POND/$FIL.reserved/$QNUM
fi

# Local reservation (less likely to be missing -- use to verify?)
if [ "$( mkdir $WORK/$FIL.reserved 2> /dev/null; echo $? )" = "0" ]
  then echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT:0:11} \t$QNUM\t$HOST \tLocking \t$RHOST \t$FIL" > $WORK?$FIL.reserved/$QNUM
fi

# Ensure the file is reserved for fetching
if [ "$( mkdir $POND/$FIL.fetched 2> /dev/null; echo $? )" = "0" ]
  then echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \t${RHOST^^} ${TREE##*/} \t$DAES jobs   \t$FIL.$EXT" > $POND/$FIL.fetched/$QNUM
fi

# Attempt to reserve the file -- central reservation
if [ "$( mkdir $POND/$FIL.repaired 2> /dev/null; echo $? )" = "0" ]
  then echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \t$DUR \t$FIL.$EXT" > $POND/$FIL.repaired/$QNUM
    echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \tRepairing  \t$DIFF \t$FIL.$EXT" | tee -a $LOGS/reservations.$( date +%F )
  else echo -e "\n\t$FIL is already reserved in the central pool for transport stream repair\n" ; exit
fi

# Node reservation
if [ "$( mkdir /work/pond/$FIL.repaired 2> /dev/null; echo $? )" = "0" ]
  then echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \t$DUR \t$FIL.$EXT" > /work/pond/$FIL.repaired/$QNUM
  else echo -e "\n\t$FIL is already reserved on the local node for transport stream repair\n" ; exit
fi

# Check the file is still non-zero (may just have been sync'ed to storage and deleted)
if [ ! -s $FFIL ] ; then echo -e "\n\t$FIL is an empty file\n" ; exit ; fi

# Get the picture size (width)
SIZ="$( ~/bin/ffprobe $FFIL 2>&1 | grep Stream | grep Video | sed -r 's/.*\ ([0-9]{3,4}x[0-9]{3,4})\ .*/\1/' )"

# Get the file size (for comparison)
S0="$( stat --format=%s $WORK/$FIL.$EXT )"

# First attempt to repair the file using dvbt-ts-repair
if [ ! -f $WORK/$FIL.ts ] ; then LOOP=1

  echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \tDvbt-ts     \t$LEN secs  \t$FIL.$EXT" | tee -a $LOGS/reservations.$( date +%F )

  while [ ! -f $WORK/$FIL.ts -a $LOOP -gt 0 ] ; do LOOP=$[LOOP-1]

    $HOME/bin/dvbt-ts-repair-no-info $FFIL $WORK/$FIL.ts 2>&1> $WORK/$FIL.ts_dvbt.txt &

    # Get the PID and the start of the encoding
    DVBT=$!  PID="$DVBT"  AGE="$( date +%s )"  S1=0

    # Wait for the ts file to be created
    sleep 31.01

    # Terminate dvbt-ts-repair if the file stops growing and the process hangs
    while [ "$PID" = "$DVBT" ] ; do echo -e "\t ~ $SCRIPT ~ $QNUM ~ `date +%F\ %H:%M:%S` ~ $FIL ~ $S1 ~ \t"
      PID="$( ps x | grep -v grep | grep $FIL | grep dvbt | awk '{ print $1 }' )" ; if [ "$PID" = "" ] ; then break ; fi
      S1="$( stat --format=%s $WORK/$FIL.ts 2>/dev/null )" ; sleep 32.01 ; NOW="$(date +%s)" ; LASTED="$[NOW-$AGE]"
      S2="$( stat --format=%s $WORK/$FIL.ts 2>/dev/null )"

      # The file should be at least 1% smaller AND the process must hang before you kill it
      if [ "$S1" -eq "$S2" -a "$S1" -lt "$[$S0 - $S0/100]" ] ; then
        PID="$( ps x | grep -v grep | grep $FIL | grep dvbt | awk '{ print $1 }' )" ; if [ "$PID" = "" ] ; then break ; fi
        kill $PID ; sleep 3 ; rm -f $WORK/$FIL.ts
        echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \tNo grow \t$LASTED secs  \t$FIL.$EXT" | tee -a $LOGS/reservations.$( date +%F ) ; break
      fi
      # Terminate dvbt-ts-repair after some multiple of the duration of the video (keep it long since scratch may be slow)
      if [ "$NOW" -gt "$[AGE+$[LEN*3]]" ] ; then
        kill $PID ; sleep 3 ; rm -f $WORK/$FIL.ts
        echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \tExpired \t$LASTED secs  \t$FIL.$EXT" | tee -a $LOGS/reservations.$( date +%F ) ; break
      fi
    done ; LASTED="$[NOW-$AGE]" DAY=$( date +%F ) YAY=$( date -d "-1 day" +%F )

    # Verify the repair succeeded
    if [ -f "$WORK/$FIL.ts" ]
      then FIXDUR="$( $HOME/bin/ffprobe $WORK/$FIL.ts 2>&1 | grep Duration | sed -r 's/.*([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{2}).*/\1/' )"
        FIXDUR="$(date -ud 1970-01-01\ $FIXDUR +%s)" FIXDIFF=$[$FIXDUR-$LEN] ; if [ $FIXDIFF -gt 0 ] ; then FIXDIFF="+$FIXDIFF" ; fi

        # Look for truncated file
        if [ $FIXDIFF -lt -$MAXDROP ] ; then rm -f $WORK/$FIL.ts
          echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \tDvbt-ts     \thang "$FIXDIFF"s \t$FIL.mpg" | tee -a $LOGS/reservations.$( date +%F ) ; break
          else FFIL=$WORK/$FIL EXT=ts

            # Some repaired files give an absurdly long duration (less commonly also a problem with ts files)
            if [ $FIXDIFF -gt $[LEN+10] ] ; then FIXDIFF="n" ; fi

            # Receipt
            DVBERR="$( cat $WORK/$FIL.ts_dvbt.txt | head -n1 | cut -d":" -f2 | cut -d" " -f2-3 )" ; rm -f $WORK/$FIL.ts_dvbt.txt
	    if [ -z "$DVBERR" ] ; then DVBERR="Flawless" ; fi
            echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \tDvbt-ts     \t"$DVBERR" \t$FIL.ts" | tee -a $LOGS/reservations.$( date +%F )
        fi
      else rm -f $WORK/$FIL.ts
    fi
  done
fi # End of dvbt-ts-repair transport stream repair

# Second attempt to repair the $EXT file, using project-x -- odd failures are relatively common, so make several attempts
# See the discussion at http://forum.dvbtechnics.info/showthread.php?t=27185
if [ ! -f $WORK/$FIL.ts -a ! -f $WORK/$FIL.m2v ] ; then LOOP=3

  echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \tProjectx    \t$LEN secs  \t$FIL.$EXT" | tee -a $LOGS/reservations.$( date +%F )

  while [ ! -f $WORK/$FIL.m2v -a $LOOP -gt 0 ] ; do LOOP=$[LOOP-1]

#    $HOME/bin/projectx -tots $FFIL -out ${FFIL%/*} -name $FIL.ts 2>&1> /dev/null &
    $HOME/bin/projectx -tots $FFIL -out ${FFIL%/*} -name $FIL.ts #2>&1> /dev/null &

    # Get the PID and the start of the encoding
    PROJECTX=$!  PID="$PROJECTX"  AGE="$( date +%s )"  S1=0

    # Wait for the ts file to be created
    sleep 61.02

    # Terminate projectx if the file stops growing, but only if the file is smaller than the $EXT file
    while [ "$PID" = "$PROJECTX" ] ; do echo -e "\t ~ $SCRIPT ~ $QNUM ~ `date +%F\ %H:%M:%S` ~ $FIL ~ $S1 ~ \t"
      PID="$( ps x | grep -v grep | grep $FIL | grep projectx | awk '{ print $1 }' )" ; if [ "$PID" = "" ] ; then break ; fi
      S1="$( stat --format=%s $WORK/$FIL*remux*.ts 2>/dev/null )" ; sleep 62.02 ; NOW="$(date +%s)" ; LASTED="$[NOW-$AGE]"
      S2="$( stat --format=%s $WORK/$FIL*remux*.ts 2>/dev/null )"
      # Some ts files are actually smaller than the $EXT file, and they can take a while to finalize
      if [ "$S1" -eq "$S2" -a "$S1" -lt "$[$S0 - $S0/100]" ] ; then
        kill $PID ; sleep 3 ; echo "rm -f $WORK/$FIL*remux*.ts $WORK/$FIL.ts_log.txt"
        echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \tNo grow \t$LASTED secs  \t$FIL.$EXT" | tee -a $LOGS/reservations.$( date +%F ) ; break
      fi

      # Terminate projectx after some multiple of the duration of the video (keep it long since scratch may be slow)
      if [ "$NOW" -gt "$[AGE+$[LEN*3]]" ] ; then
        kill $PID ; sleep 3 ; rm -f $WORK/$FIL*remux*.ts $WORK/$FIL.ts_log.txt
        echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \tExpired \t$LASTED secs  \t$FIL.$EXT" | tee -a $LOGS/reservations.$( date +%F ) ; break
      fi
    done ; LASTED="$[NOW-$AGE]" DAY=$( date +%F ) YAY=$( date -d "-1 day" +%F )

    # Verify the repair succeeded and try again if not (often works!)
    if [ -f $WORK/$FIL*remux*.ts ]
      then FIXDUR="$( $HOME/bin/ffprobe $WORK/$FIL*remux*.ts 2>&1 | grep Duration | sed -r 's/.*([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{2}).*/\1/' )"
        FIXDUR="$(date -ud 1970-01-01\ $FIXDUR +%s)" FIXDIFF=$[$FIXDUR-$LEN] ; if [ $FIXDIFF -gt 0 ] ; then FIXDIFF="+$FIXDIFF" ; fi
        # Look for truncated file
        if [ $FIXDIFF -lt -$MAXDROP ] ; then rm -f $WORK/$FIL*remux*.ts $WORK/$FIL.ts_log.txt

            # Log "Drop" the first time and "DROP" the second -- only DROP triggers note in CMT field
            if [ "$( grep -h $FIL $LOGS/reservations.{$YAY,$DAY} | grep Projectx | grep Drop )" ]
              then echo -e "$(date +%F\ %H:%M:%S) \t${SCRIPT%.*} \t$QNUM\t$HOST \tProjectx \tDROP "$FIXDIFF"s \t$FIL" | tee -a $LOGS/reservations.$( date +%F )
              else echo -e "$(date +%F\ %H:%M:%S) \t${SCRIPT%.*} \t$QNUM\t$HOST \tProjectx \tDrop "$FIXDIFF"s \t$FIL" | tee -a $LOGS/reservations.$( date +%F ) ; continue
            fi

          else mv -n $WORK/$FIL*remux*.ts $WORK/$FIL.m2v ; FFIL=$WORK/$FIL EXT=m2v

            # Some repaired files give an absurdly long duration (less commonly also a problem with ts files)
            if [ $FIXDIFF -gt $[LEN+10] ] ; then FIXDIFF="n" ; fi

            # Receipt
            FIXNUM="$( grep '> we have' $WORK/$FIL.ts_log.txt | tail -1 | sed -r 's/.*([0-9]{1,}).*/\1/' )" ; rm -f $WORK/$FIL.ts_log.txt
            echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \tProjectx   \t$FIXNUM fixes "$FIXDIFF"s \t$FIL.m2v" | tee -a $LOGS/reservations.$( date +%F )
        fi


      else #$WORK/$FIL.ts_log.txt"
        # Log "Retry" the first time and "FATAL" the second -- only FATAL triggers the search for an alternate by recover-daemon.sh
        if [ "$( grep -h $FIL $LOGS/reservations.{$YAY,$DAY} | grep Projectx | grep Retry )" ]
          then echo -e "$(date +%F\ %H:%M:%S) \t${SCRIPT%.*} \t$QNUM\t$HOST \tProjectx \tFATAL "$LASTED"s \t$FIL" | tee -a $LOGS/reservations.$( date +%F ) ; exit
          else echo -e "$(date +%F\ %H:%M:%S) \t${SCRIPT%.*} \t$QNUM\t$HOST \tProjectx \tRetry "$LASTED"s\t$FIL" | tee -a $LOGS/reservations.$( date +%F ) ; continue
        fi
    fi
  done
fi # End of project-x transport stream repair

# After Project-x, try again with dvbt-ts-repair
if [ -f $WORK/$FIL.m2v ] ; then

  echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \tDvbt-ts     \t$LEN secs  \t$FIL.m2v" | tee -a $LOGS/reservations.$( date +%F )

  while [ ! -f $WORK/$FIL.ts -a $LOOP -gt 0 ] ; do LOOP=$[LOOP-1]

    $HOME/bin/dvbt-ts-repair-no-info $WORK/$FIL.m2v $WORK/$FIL.ts 2>&1> $WORK/$FIL.ts_dvbt.txt &

    # Get the PID and the start of the encoding
    DVBT=$!  PID="$DVBT"  AGE="$( date +%s )"  S1=0

    # Wait for the ts file to be created
    sleep 31.01

    # Terminate dvbt-ts-repair if the file stops growing and the process hangs
    while [ "$PID" = "$DVBT" ] ; do echo -e "\t ~ $SCRIPT ~ $QNUM ~ `date +%F\ %H:%M:%S` ~ $FIL ~ $S1 ~ \t"
      PID="$( ps x | grep -v grep | grep $FIL | grep dvbt | awk '{ print $1 }' )" ; if [ "$PID" = "" ] ; then break ; fi
      S1="$( stat --format=%s $WORK/$FIL.ts 2>/dev/null )" ; sleep 32.01 ; NOW="$(date +%s)" ; LASTED="$[NOW-$AGE]"
      S2="$( stat --format=%s $WORK/$FIL.ts 2>/dev/null )"

      # The file should be at least 1% smaller AND the process must hang before you kill it
      if [ "$S1" -eq "$S2" -a "$S1" -lt "$[$S0 - $S0/100]" ] ; then
        PID="$( ps x | grep -v grep | grep $FIL | grep dvbt | awk '{ print $1 }' )" ; if [ "$PID" = "" ] ; then break ; fi
        kill $PID ; sleep 3 ; rm -f $WORK/$FIL.ts
        echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \tNo grow \t$LASTED secs  \t$FIL.$EXT" | tee -a $LOGS/reservations.$( date +%F ) ; break
      fi
      # Terminate dvbt-ts-repair after some multiple of the duration of the video (keep it long since scratch may be slow)
      if [ "$NOW" -gt "$[AGE+$[LEN*3]]" ] ; then
        kill $PID ; sleep 3 ; rm -f $WORK/$FIL.ts
        echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \tExpired \t$LASTED secs  \t$FIL.$EXT" | tee -a $LOGS/reservations.$( date +%F ) ; break
      fi
    done ; LASTED="$[NOW-$AGE]" DAY=$( date +%F ) YAY=$( date -d "-1 day" +%F )

    # Verify the repair succeeded
    if [ -f "$WORK/$FIL.ts" ]
      then FIXDUR="$( $HOME/bin/ffprobe $WORK/$FIL.ts 2>&1 | grep Duration | sed -r 's/.*([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{2}).*/\1/' )"
        FIXDUR="$(date -ud 1970-01-01\ $FIXDUR +%s)" FIXDIFF=$[$FIXDUR-$LEN] ; if [ $FIXDIFF -gt 0 ] ; then FIXDIFF="+$FIXDIFF" ; fi

        # Look for truncated file
        if [ $FIXDIFF -lt -$MAXDROP ] ; then rm -f $WORK/$FIL.ts
          echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \tDvbt-ts     \thang "$FIXDIFF"s \t$FIL.mpg" | tee -a $LOGS/reservations.$( date +%F ) ; break
          else FFIL=$WORK/$FIL EXT=ts

            # Some repaired files give an absurdly long duration (less commonly also seen in mpg files)
            if [ $FIXDIFF -gt $[LEN+10] ] ; then FIXDIFF="n" ; fi

            # Receipt
            DVBERR="$( cat $WORK/$FIL.ts_dvbt.txt | head -n1 | cut -d":" -f2 | cut -d" " -f2-3 )" ; rm -f $WORK/$FIL.ts_dvbt.txt
	    if [ -z "$DVBERR" ] ; then DVBERR="Flawless" ; fi
            echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \tDvbt-ts     \t"$DVBERR" \t$FIL.ts" | tee -a $LOGS/reservations.$( date +%F )
        fi
    fi
  done
fi # End of dvbt-ts-repair transport stream repair

# Remove the intermediate Project-x file
if [ -f $WORK/$FIL.ts ] ; then rm -f $WORK/$FIL.m2v ; fi

# Remove the primary reservation
rm -rf $POND/$FIL.repaired

# Remove the secondary reservation
rm -rf $R2DIR/$FIL.repaired

# Processing time
PTIM="$(date -ud "+$[`date +%s`-START] seconds"\ $(date +%F) +%H:%M:%S)"

# Receipt
echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \tRepair time\t$PTIM     \t$FIL" | tee -a $LOGS/reservations.$( date +%F )

# EOF
