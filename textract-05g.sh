#!/bin/bash
#
# Extract the timestamped closed captioning from an mpeg-ts file.
#
# Written on 2012-02-08 (Kai Chan/FFS)
#
# Dependencies:
#
#	ccextractor 0.79 or higher; calls cc-extract-bin.sh
#
# Changelog:
#
#	2016-02-23 Add srt output for possible integration into the mp4 file
#	2016-02-11 Add cc-extract-bin.sh and tag-commercials.sh
#	2016-02-10 Rename cc-extract-la.sh to textract.name -- remove non-hoffman code, write to log, needs 0.79+
#	2014-06-19 Generate a .txt2 file while testing on hoffman2
#	2014-06-03 Improve removing spaces after RU2|
#	2014-05-26 Update to run on local nodes on hoffman2
#	2014-04-15 Remove any intrusive 7F DEL character
#	2014-04-12 Prefer the ts file -- post-repair has better timestamps
#	2014-04-11 Remove any byte order mark (BOM)
#	2014-04-05 Use ccextractor 0.69 (fixes PCR clock rollover)
#	2013-07-23 Use ccx.bin (metadata dump) if video is missing; use uuid or uuidgen
#	2013-07-19 Forked from cc-extract-ia.sh for UCLA files, add ability to use existing header
#	2013-07-15 Customized for processing Internet Archive files on Hoffman2
#	2013-07-14 Use ccextractor 0.67-a2 (-UCLA timestamps)
#	2013-06-16 Use ccextractor 0.66-a5 (handles both -pn and -autoprogram)
#	2013-06-09 Use ccextractor 0.66-a4 (symlinked to ccextractor, new flags -autoprogram and -UCLA)
#	2012-10-12 Change GMT to UTC and add -utf8
#	2012-05-25 Add ISO country code
#	2012-02-12 Use mpg rather than m2p files (tentative)
#	2012-02-08 Forked from fix_cc_with_ccextractor
#
# ------------------------------------------------------------------------------

# Script name
SCRIPT=`basename $0`

# Help screen
if [ "$1" = "-h" -o "$1" = "--help" -o "$1" = "help" ]
 then echo -e "\n\tExtract the closed captioning from a transport stream recorded at UCLA"
  echo -e "\n\t\t$SCRIPT <filename> [<program number>]"
  echo -e "\n\tExample:"
  echo -e "\n\t\t$SCRIPT 2012-02-06_1800_FOX-News_Hannity"
  echo -e "\nThe script looks for ts, mpg, and ccx.bin files"
  echo -e "\n\tCreates a header if needed.\n"
   exit
fi

# Filename
if [ -z $1 ]
  then echo -e "\n\tUsage: $0 <filename> [<program number>]\n" ; exit
  else FIL="$1"
fi

# Host
HOST=$( hostname -s )

# Manual override for home directory
HOME=/u/home/g/groeling

# Manual override of scratch directory
SCRATCH=$HOME/tv2

# Log directory
LOGS=$SCRATCH/logs

# Working directory
WORK=/work/pond

# Local day directory
TV=/work/day

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

# If FIL included a directory, keep it
if [ "${FIL%/*}" != "$FIL" ] ; then DIR="${FIL%/*}" ; fi

# Strip path
FIL=${FIL##*/}

# If FIL included an extension, keep it
if [ "${FIL#*.}" != "$FIL" ] ; then EXT=${FIL#*.} ; else EXT=ts ; fi

# Strip extension
FIL=${FIL%.*}

# Get the day tree
DDIR="$(echo $FIL | sed -r 's/([0-9]{4})-([0-9]{2})-([0-9]{2}).*/\1\/\1-\2\/\1-\2-\3/')"

# File existence and extension
if [ -f $WORK/$FIL.ts ] ; then EXT=ts CMT=""
  elif [ -f $WORK/$FIL.mpg ] ; then EXT=mpg CMT="Repair failed"
  elif [ -f $TV/$DDIR/$FIL.ccx.bin ] ; then echo -e "\n\tUnable to find $FIL.ts or mpg, and ccx.bin already exists\n" ; exit
    else echo -e "\n\tUnable to find $FIL.ts, mpg, or ccx.bin\n" ; exit
fi

# Look for the source file
if [ ! -f "$WORK/$FIL.$EXT" ] ; then
  if [ -f "$TV/$DDIR/$FIL.$EXT" ]
    then WORK=$TV/$DDIR
    else echo -e "\n\tNo $FIL.$EXT found\n" ; exit
  fi
fi

# Debug
#echo -e "FIL is $FIL and DIR is $WORK and DDIR is $DDIR"

# Node reservation
if [ "$( mkdir /work/pond/$FIL.textracted 2> /dev/null; echo $? )" = "0" ]
  then echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \t$DUR \t$FIL.$EXT" > /work/pond/$FIL.textracted/$QNUM
  else echo -e "\n\t$FIL is already reserved for text extraction on the local node\n" ; exit
fi

# Walk into the local day directory
mkdir -p $TV/$DDIR ; cd $TV/$DDIR

pwd

# A. Create a new header file

# Get the components of the file name
eval "$( echo "$FIL" | sed -r 's/[0-9_-]{15}_([A-Z]{2})_([A-Za-z0-9-]{2,20})_(.*)/CTRY=\1 NWK=\2 SHW=\3/' )"

# Program ID and language (you could put this in a parameter file)
case $NWK in
  AlJazeera     ) PN=6    ; LANG=ENG ;;
  CNN           ) PN=1    ; LANG=ENG ;;
  CSPAN         ) PN=2    ; LANG=ENG ;;
  CSPAN         ) PN=3    ; LANG=ENG ;;
  ComedyCentral ) PN=2    ; LANG=ENG ;;
  FOX-News      ) PN=2    ; LANG=ENG ;;
  HLN           ) PN=1    ; LANG=ENG ;;
  KABC          ) PN=1    ; LANG=ENG ;;
  KCBS          ) PN=1    ; LANG=ENG ;;
  KCAL          ) PN=1    ; LANG=ENG ;;
  KCET          ) PN=1    ; LANG=ENG ;;
  KMEX          ) PN=4    ; LANG=SPA ;;
  KNBC          ) PN=3    ; LANG=ENG ;;
  KOCE          ) PN=2    ; LANG=ENG ;;
  KTLA          ) PN=1    ; LANG=ENG ;;
  KTTV-FOX      ) PN=3    ; LANG=ENG ;;
  MSNBC         ) PN=4    ; LANG=ENG ;;
  RTP-1         ) PN=1101 ; LANG=POR ;;
  RTP-2         ) PN=1102 ; LANG=POR ;;
  SIC           ) PN=1103 ; LANG=POR ;;
  TV1           ) PN=1104 ; LANG=POR ;;
  WEWS          ) PN=1    ; LANG=ENG ;;
  WKYC          ) PN=1    ; LANG=ENG ;;
  WOIO          ) PN=2    ; LANG=ENG ;;
  WUAB          ) PN=7    ; LANG=ENG ;;
  TV5           ) echo "$END" >> $FIL.txt1 ; exit ;;
  *             ) PN=""   ; LANG=ENG ;;
esac

# Manual program number?
if [ -n "$2" ] ; then PN="$2" ; fi

# Get the name of any existing file with a header
HED="$( grep -l ^'TOP' $FIL.{tmp,txt,tlx}* 2>/dev/null | head -n 1 )"

# Baseline UTC time based on file name
BTIM="$( echo $FIL | sed -r s/'([0-9]{4}-[0-9]{2}-[0-9]{2})_([0-9]{2})([0-9]{2}).*/\1\ \2:\3:00/' )"

# Local library for ffprobe
export LD_LIBRARY_PATH+=:$HOME/lib/

# Create a new header file, from scratch if needed
if [ -n "$HED" ]

  then

    # Clip the header lines and create a new header file
    HEAD="$( sed -n '/^LBT/=' $HED | sed '1q' )"
    OFS=$IFS IFS=$'\n' HEAD=( `head -n $HEAD $HED` )

    # Until you really have this working, do not mess with the original txt file
    #if [ -f $FIL.txt ] ; then mv --backup=numbered $FIL.txt $FIL.txt-orig ; fi

    # Instead, create a .txt1 file
    for LIN in "${HEAD[@]}"; do echo "$LIN" >> $FIL.txt1 ; done ; IFS=$OFS

    # Verify the duration is set (should be done by a check-cc-* script)
    if [ "$( grep ^'DUR|' $FIL.txt2 | cut -d"|" -f2 )" = "" ] ; then
      LEN="$( $HOME/bin/ffprobe $WORK/$FIL.$EXT 2>&1 | grep Duration | sed -rn s/'.*([0-9]{1}:[0-9]{1,2}:[0-9]{1,2}\.[0-9]{2,3}).*/\1/p' )"
      sed -i -r "/DUR\|/cDUR|$LEN" $FIL.txt1
    fi

  else

    # Define the first timestamp
    echo "TOP|$( date -d "$BTIM" +%Y%m%d%H%M%S )|$FIL" > $FIL.txt1

    # Generate the video length tag (should perhaps be done with the finished mp4 file?)
    # Only print the modified line (see http://www.grymoire.com/Unix/Sed.html#uh-9)
    LEN="$( $HOME/bin/ffprobe $WORK/$FIL.$EXT 2>&1 | grep Duration | sed -rn s/'.*([0-9]{1}:[0-9]{1,2}:[0-9]{1,2}\.[0-9]{2,3}).*/\1/p' )"

    # Add the collection name, an identifier, and the video duration (insert after the first timestamp)
    sed -i "1a`echo "COL|Communication Studies Archive, UCLA\nUID|$(uuidgen)\nDUR|"$LEN""`" $FIL.txt1

    # Assume Los Angeles recording location for UCLA files
    LBT=$( eval "date -d 'TZ=\"America/Los_Angeles\" $BTIM' '+%Y-%m-%d %H:%M:00'")" America/Los_Angeles"

    # Add any comment and the local broadcast time and timezone
    echo -e "SRC|"Rosenthal Collection, UCLA"\nCMT|$CMT\nLAN|$LANG\nLBT|$LBT" >> $FIL.txt1

fi

# Get the end line
if [ "$END" = "" ] ; then END="$( grep ^'END|' $FIL.{tmp,txt,tlx}* 2>/dev/null | cut -d":" -f2 | head -n 1 )" ; fi

# If none is available, try to synthesize (is this actually working?)
if [ "$END" = "" ] ; then

  # Get the duration from the header
  if [ "$LEN" = "" ] ; then LEN="$( grep ^'DUR|' $FIL.{tmp,txt,tlx}* 2>/dev/null | cut -d"|" -f2 | head -n 1 )" ; fi

  # Convert the duration to seconds
  SECS="$(echo $(date -u -d 1970-01-01\ $LEN +%s))"

  # Add the number of seconds to the initial time
  LTIM=$( date -d "+$SECS seconds"\ "$BTIM" +%F\ %H:%M:%S)

  # Define the last timestamp, converting spaces to underscores
  END="END|$( date -d "$LTIM" +%Y%m%d%H%M%S )|$FIL"

fi

# Get the source
RHOST="$( cut -d" " -f1 $HOME/tv2/pond/$FIL.len )"

# B. Time conversion

# Extract the start date/time from the TOP line of any file that has the extension .t*
top_date_str=`grep -E '^TOP\|' $FIL.t* | head -n 1 | awk -F'|' '{print \$2}'`

# Convert the start time from YYYYMMDDHHMMSS into YYYY-MM-DD HH:MM:SS format
top_date_str2=`echo $top_date_str | sed -r 's/([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})/\1-\2-\3 \4:\5:\6/'`

# Parse the date into the number of seconds since epoch and add milliseconds
top_date_int="`date --date="$top_date_str2 UTC" '+%s'`000"

# Welcome
echo -en "\n   Ready to extract the captions from $FIL.$EXT ..." ; sleep 2 ; echo -e "\n"

# C. Extract the closed captioning

# Call CCExtractor
# -ts                                  - input is a MPEG TS file
# -datets			       - write time as YYYYMMDDHHMMss.nnn
# -autoprogram                         - automatically select the first good program ID
# -pn $PN                              - select a known program ID
# -1 or -2 or -12                      - primary transcript, secondary transcript, or both
# -UCLA                                - adds CSA date format (?)
# -noru                                - do not repeat lines to simulate roll-up
# -out=ttxt                            - plain text output format
# -utf8                                - character set
# -unixts                              - unix-captured transport stream?
# -delay $top_date_int                 - set the absolute start time (number of seconds since epoch)
# -o1 $FIL.ccx.out1   - named first and second output (if any) -- ccextractor defaults to $FIL.txt
# $FIL.$EXT                            - input file (ts, mpg, or ccx.bin)

# Clean up from previous attempts
rm -f $FIL.ccx.out{1,2,3} $FIL.txt3 ; cp -p $FIL.txt1 $FIL.txt2

# Metadata extraction
$HOME/bin/ccextractor -ts -pn $PN -out=bin -o $FIL.ccx.bin $WORK/$FIL.$EXT > $FIL.bin.stout 2> $FIL.bin.stderr

# If the program number fails, try autoprogram
if [ -s $FIL.ccx.bin ] ; then BZE="$( du -k $FIL.ccx.bin | cut -f1 )" ; else BZE=0 ; fi
if [ ! -s $FIL.ccx.bin -o "$BZE" -le "1" ] ; then
  echo -e "Extracting metadata from program number $PN failed -- trying autoprogram ..."
  $HOME/bin/ccextractor -ts -autoprogram -out=bin -o $FIL.ccx.bin $WORK/$FIL.$EXT > $FIL.ccx.stout 2> $FIL.ccx.stderr
fi

# Delete any empty .bin.stout and .bin.stderr files
if [ ! -s "$FIL.bin.stdout" ] ; then rm -f $FIL.bin.stdout ; fi
if [ ! -s "$FIL.bin.stderr" ] ; then rm -f $FIL.bin.stderr ; fi

# Receipt metadata extraction
if [ -s $FIL.ccx.bin ] ; then BZE="$( du -k $FIL.ccx.bin | cut -f1 )" ; else BZE=0 ; fi
if [ ! -s $FIL.ccx.bin -o "$BZE" -le "100" ]
  then echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \tMetadata ${RHOST^^} \tNone          \t$FIL.ccx.bin" | tee -a $LOGS/reservations.$( date +%F )
  else echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \tMetadata ${RHOST^^} \t$BZE mb       \t$FIL.ccx.bin" | tee -a $LOGS/reservations.$( date +%F )
fi

# Text extraction: try the known program number first
if [ "$EXT" = "mpg" -o "$EXT" = "ts" ] ; then
  echo -e "\tExtracting program number $PN from the transport stream ..."
  $HOME/bin/ccextractor -datets -pn $PN -UCLA -12 -noru -ttxt -utf8 -unixts 0 -delay $top_date_int -o $FIL.ccx.out1 $WORK/$FIL.$EXT > $FIL.ccx.stout 2> $FIL.ccx.stderr
  $HOME/bin/ccextractor -datets -pn $PN -noru -utf8 -o $FIL.srt $WORK/$FIL.$EXT > $FIL.ccx.srtout 2> $FIL.ccx.srtderr
fi

# If extracting $PN from the transport stream fails, try $PN from the metadata dump
if [ -s $FIL.ccx.out1 ] ; then OUTSIZE="$( stat -c%s $FIL.ccx.out1 )" ; else OUTSIZE=0 ; fi
if [ "$OUTSIZE" -le "100" ] ; then
  if [ -f $FIL.ccx.bin ] ; then
    echo -e "\tExtracting program number $PN from the transport stream failed -- trying program number $PN from the metadata dump ..."
    $HOME/bin/ccextractor -in=bin -pn $PN -UCLA -12 -noru -out=ttxt -utf8 -unixts 0 -delay $top_date_int -o $FIL.ccx.out1 $FIL.ccx.bin > $FIL.ccx.stout 2> $FIL.ccx.stderr
    $HOME/bin/ccextractor -in=bin -pn $PN -noru -utf8 -o $FIL.srt $FIL.ccx.bin > $FIL.ccx.srtout 2> $FIL.ccx.srtderr
  fi
fi

# If the program number fails, try autoprogram
if [ -s $FIL.ccx.out1 ] ; then OUTSIZE="$( stat -c%s $FIL.ccx.out1 )" ; else OUTSIZE=0 ; fi
if [ "$OUTSIZE" -le "100" -a \( "$EXT" = "mpg" -o "$EXT" = "ts" \) ] ; then
    echo -e "\tExtracting program number $PN from the metadata dump failed -- trying autoprogram from the transport stream ..."
    $HOME/bin/ccextractor -ts -autoprogram -UCLA -12 -noru -out=ttxt -utf8 -unixts 0 -delay $top_date_int -o $FIL.ccx.out1 $WORK/$FIL.$EXT > $FIL.ccx.stout 2> $FIL.ccx.stderr
    $HOME/bin/ccextractor -ts -autoprogram -noru -utf8 -o $FIL.srt $WORK/$FIL.$EXT > $FIL.ccx.srtout 2> $FIL.ccx.srtderr
fi

# If extracting $PN from the metadata dump fails, try autoprogram from the metadata dump
if [ -s $FIL.ccx.out1 ] ; then OUTSIZE="$( stat -c%s $FIL.ccx.out1 )" ; else OUTSIZE=0 ; fi
if [ -f $FIL.ccx.bin -a "$OUTSIZE" -le "100" ]
  then echo -e "\tExtracting autoprogram from the transport stream failed -- trying autoprogram from the metadata dump ..."
    $HOME/bin/ccextractor -in=bin -autoprogram -UCLA -12 -noru -out=ttxt -utf8 -unixts 0 -delay $top_date_int -o $FIL.ccx.out1 $FIL.ccx.bin > $FIL.ccx.stout 2> $FIL.ccx.stderr
    $HOME/bin/ccextractor -in=bin -autoprogram -noru -utf8 -o $FIL.srt $FIL.ccx.bin > $FIL.ccx.srtout 2> $FIL.ccx.srtderr
fi

# Second captions track
if [ ! -s $FIL.ccx_2.txt ] ; then rm -f $FIL.ccx_2.txt ; else mv $FIL.ccx_2.txt $FIL.ccx.out2 ; fi                          # From 0.78?
if [ ! -s $WORK/$FIL.ccx_2.txt ] ; then rm -f $WORK/$FIL.ccx_2.txt ; else mv $WORK/$FIL.ccx_2.txt $WORK/$FIL.ccx.out2 ; fi  # Before 0.78
if [ ! -s $WORK/$FIL\_2.txt ] ; then rm -f $WORK/$FIL\_2.txt ; else mv $WORK/$FIL\_2.txt $WORK/$FIL.ccx.out2 ; fi           # From 0.74?

# Delete any empty .ccx.stout and .ccx.stderr files
if [ ! -s "$FIL.ccx.stdout" ]  ; then rm -f $FIL.ccx.stdout ; fi
if [ ! -s "$FIL.ccx.stderr" ]  ; then rm -f $FIL.ccx.stderr ; fi
if [ ! -s "$FIL.ccx.srtdout" ] ; then rm -f $FIL.ccx.srtdout ; fi
if [ ! -s "$FIL.ccx.srtderr" ] ; then rm -f $FIL.ccx.srtderr ; fi

# Convert from DOS format
if [ -f $FIL.ccx.out1 ] ; then dos2unix -q -o $FIL.ccx.out1 ; fi
if [ -f $FIL.ccx.out2 ] ; then dos2unix -q -o $FIL.ccx.out2 ; fi
if [ -f $FIL.srt ]      ; then dos2unix -q -o $FIL.srt  ; fi

# Remove any byte order mark (BOM)
if [ -f $FIL.ccx.out1 ] ; then sed -i -e '1s/^\xef\xbb\xbf//' $FIL.ccx.out1 ; fi
if [ -f $FIL.ccx.out2 ] ; then sed -i -e '1s/^\xef\xbb\xbf//' $FIL.ccx.out2 ; fi
if [ -f $FIL.srt ]      ; then sed -i -e '1s/^\xef\xbb\xbf//' $FIL.srt  ; fi

# Remove any 7F DEL character
if [ -f $FIL.ccx.out1 ] ; then sed -i -e 's/\x7F//g' $FIL.ccx.out1 ; fi
if [ -f $FIL.ccx.out2 ] ; then sed -i -e 's/\x7F//g' $FIL.ccx.out2 ; fi
if [ -f $FIL.srt ]      ; then sed -i -e 's/\x7F//g' $FIL.srt  ; fi

# Combine the primary with the secondary transcript, if the secondary has at least 50 lines that are different
if [ -s $FIL.ccx.out2 ] ; then DIFF="$( diff --changed-group-format='%<' --unchanged-group-format='' $FIL.ccx.out{2,1} | wc -l | xargs )"
  if [ "$DIFF" -gt "50" ] ; then sed 's/CC1/CC2/' < $FIL.ccx.out2 > $FIL.ccx.out3
    cat $FIL.ccx.out1 >> $FIL.ccx.out3
    sort $FIL.ccx.out3 > $FIL.ccx.out1
  fi
fi

# Remove extra space after regular caption style (affects The_OReilly_Factor and AlJazeera)
sed -i 's/RU2|[ ]*/RU2|/' $FIL.ccx.out1

# Add CC to the header
cat $FIL.ccx.out1 >> $FIL.txt2

# Add the last timestamp
echo "$END" >> $FIL.txt2

# Convert from DOS format (not needed)
dos2unix -q -k -o $FIL.txt2

# Receipt
if [ -s $FIL.txt2 ] ; then SIZE="$( grep '^2' $FIL.txt2 | wc -l )" ; else SIZE=0 ; fi
if [ ! -s $FIL.txt2 -o "$SIZE" -le "1" ]
  then sed -i -r "/CMT\|/cCMT|Text extraction failed" $FIL.txt2
       echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \tCaptions ${RHOST^^} \t0 lines  \t$FIL.txt2" | tee -a $LOGS/reservations.$( date +%F )
  else echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \tCaptions ${RHOST^^} \t$SIZE lines \t$FIL.txt2" | tee -a $LOGS/reservations.$( date +%F )
fi

# C. Tag commercials

# Written by FFS on 14 Oct 2013
#
# Dependencies: sponge (moreutils)
#
# Changelog:
#
#	2014-06-24 Use extension txt2 on hoffman
#	2014-04-12 Corner case -- tag ends in commercial
#	2014-04-11 Fix failure to detect POP only
#	2014-02-26 Skip triple chevrons in CC3 lines (Spanish)
#       2014-02-23 Add story boundary on triple chevrons
#	2014-01-05 Use the start of the next story rather than the end of the POP to define the commercial block
#
#------------------------------------------------------------------------------------

# What does this third section do
# echo -e "\n\tConvert caption style information to segment tags for commercials"
# echo -e "\tand add story start tags before triple chevrons (>>>)."
# echo -e "\n\tThe original file is given the extension cs (caption styles)."

# Verify extension
#if [ ! -f $FIL.txt2 ] ; then echo -e "\n\tThe commercial tagger processes .txt2 files.\n" ; exit ; fi

# Process files that have caption styles and triple chevrons
if [ "$( egrep -m 1 "\|RU2\||\|RU3\||\|POP\||\|>>>" $FIL.txt2 )" ] ; then

  # Welcome
  echo -en "\tCreating commercial and story tags for $FIL.txt2\t"

  # Host system
  HOST="$( hostname -s )"

  # File length
  NLIN="$( cat $FIL.txt2 | wc -l )"

  # Internal field separator
  OFS=$IFS

  # Examine the file a line at a time
  for N in `seq 1 $NLIN` ; do

    # Debug
    #if [ "$N" -gt "20" ] ; then break ; fi

    # Capture the line
    read LIN <<< $( sed -n "$N p" $FIL.txt2 )

    # At the end of the file
    if [ "${LIN:0:3}" = "END" ] ; then

      # If we end in the middle of a commercial (corner case)
      if [ "$SAD" != "" -a "$EAD" != "" ] ; then

        # Use the end time of the last POP line
        SEG="$SAD|$EAD|SEG_00|Type=Commercial"

        # Insert the commercial block tag before the start
        sed -i "1,/^$SAD|$SEAD/ {/^$SAD|$SEAD/i\
$SEG
}" $FIL.txt3
        SAD="" ; EAD="" ; echo -en "."
      fi

      # Write the END line
      echo -e "$LIN" >> $FIL.txt3 ; continue
    fi

    # Keep the lines that start with a letter (header)
    if [[ "${LIN:0:1}" =~ [A-Z] ]] ; then echo -e "$LIN" >> $FIL.txt3 ; continue ; fi
   #if [[ "${LIN:0:1}" =~ [A-Z] ]] ; then echo -e "$LIN" | tee -a $FIL.txt3 ; continue ; fi

    # Capture the field values in each line (for all other lines) in an array
    IFS=$'\n' ; FLD=( $( echo "$LIN" | sed -e 's/|/\n/g' ) )

    # Rewrite non-commercial lines
    if [ "${FLD[3]}" != "POP" ] ; then

      # Initial story start
      if [ -z "$FIRST" ] ; then FIRST=$N ; echo "${FLD[0]}|${FLD[1]}|SEG_00|Type=Story start" >> $FIL.txt3

        # Get the starting timestamp of a triple chevron (>>>) indicating a story boundary -- but not in US Spanish files
        #elif [[ "${FLD[4]}" =~ ">>>" && "${FLD[2]}" != "CC3" && $FIL != *KMEX* ]] ; then echo "${FLD[0]}|${FLD[1]}|SEG_00|Type=Story start" >> $FIL.txt3
        elif [[ "${FLD[4]}" =~ ">>>" && "${FLD[2]}" != "CC3" ]] ; then echo "${FLD[0]}|${FLD[1]}|SEG_00|Type=Story start" >> $FIL.txt3

      fi

      echo "${FLD[0]}|${FLD[1]}|${FLD[2]}|${FLD[4]}" >> $FIL.txt3
    fi

    # Get the start and end time of the first line of the commercial
    if [ "${FLD[3]}" = "POP" -a "$SAD" = "" ] ; then SAD="${FLD[0]}" SEAD="${FLD[1]}" ; fi

    # Rewrite the commercial lines and store the successive end times
    if [ "${FLD[3]}" = "POP" ] ; then echo "${FLD[0]}|${FLD[1]}|${FLD[2]}|${FLD[4]}" >> $FIL.txt3 ; EAD="${FLD[1]}" ; fi

    # Debug
    #echo -e "\n\t{FLD[3]} is ${FLD[3]} and EAD is $EAD\n"

    # Get the end of the commercial
    if [ "${FLD[3]}" != "POP" -a "$EAD" != "" ] ; then

      # Either use the end time of the last POP line
      #SEG="$SAD|$EAD|SEG_00|Type=Commercial"

      # Or better, the start time of the first non-POP line
      SEG="$SAD|${FLD[0]}|SEG_00|Type=Commercial"

      # Insert the commercial block tag before the start
      sed -i "1,/^$SAD|$SEAD/ {/^$SAD|$SEAD/i\
$SEG
}" $FIL.txt3
      SAD="" ; EAD="" ; echo -en "."

      # At the same time, insert a single story start tag at the end of the commercial
      SEG="${FLD[0]}|${FLD[1]}|SEG_00|Type=Story start"
      sed -i "1,/^${FLD[0]}|${FLD[1]}/ {/^${FLD[0]}|${FLD[1]}/i\
$SEG
}" $FIL.txt3

    fi

  done

fi

# Internal field separator
IFS=$OFS

# For files that had no caption styles
if [ ! -e $FIL.txt3 ] ; then cp -p $FIL.txt2 $FIL.txt3 ; fi

# Remove duplicate lines (SEG lines after POP and before >>>)
uniq $FIL.txt3 | $HOME/bin/sponge $FIL.txt3

# Receipt
if [ -s $FIL.txt3 ] ; then TAGS="$( grep -c SEG_00 $FIL.txt3 )" ; else SIZE=0 ; fi
echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \tCommercials \t$TAGS tags  \t$FIL.txt3" | tee -a $LOGS/reservations.$( date +%F )

# Copy the text to storage -- minimally the header
cp -p $FIL.txt3 $HOME/tv2/day/$DDIR/
COPY1="$( $HOME/bin/copy2csa-02.sh $FIL.txt3 | tail -n2 | grep OK )"

# Try the loopback script if needed
if [[ $COPY1 != OK* ]] ; then COPY1="$( $HOME/bin/copy2csa-00.sh $FIL.txt3 | tail -n2 | grep OK )" ; fi

# Try roma if cartago fails
if [[ $COPY1 != OK* ]] ; then COPY1="$( $HOME/bin/copy2csa-00.sh $FIL.txt3 roma /mnt/ifs/NewsScape/Rosenthal | tail -n2 | grep OK )" ; fi

# Verify the text arrived
if [[ $COPY1 != OK* ]]
  then echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \tText copy \tFAILED     \t$FIL.txt3" | tee -a $LOGS/reservations.$( date +%F )
fi

# Copy the srt to storage if it has some content
if [ "$( stat -c%s $FIL.srt )" -gt 10 ] ; then

  # Local and remote storage
  cp -p $FIL.srt $HOME/tv2/day/$DDIR/
  COPY2="$( $HOME/bin/copy2csa-02.sh $FIL.srt  | tail -n2 | grep OK )"

  # Try the loopback script if needed
  if [[ $COPY2 != OK* ]] ; then COPY2="$( $HOME/bin/copy2csa-00.sh $FIL.srt | tail -n2 | grep OK )" ; fi

  # Try roma if cartago fails
  if [[ $COPY2 != OK* ]] ; then COPY2="$( $HOME/bin/copy2csa-00.sh $FIL.srt  roma /mnt/ifs/NewsScape/Rosenthal | tail -n2 | grep OK )" ; fi

  # Verify the srt arrived
  if [[ $COPY2 != OK* ]]
    then echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \tSrt copy  \tFAILED     \t$FIL.srt" | tee -a $LOGS/reservations.$( date +%F )
  fi

fi

# Copy the metadata to storage
if [ "$( stat -c%s $FIL.ccx.bin )" -gt 1000 ] ; then

  # Local and remote storage
  cp -p $FIL.ccx.bin $HOME/tv2/day/$DDIR/
  COPY3="$( $HOME/bin/copy2csa-02.sh $FIL.ccx.bin | tail -n2 | grep OK )"

  # Try the loopback script if needed
  if [[ $COPY3 != OK* ]] ; then COPY3="$( $HOME/bin/copy2csa-00.sh $FIL.ccx.bin | tail -n2 | grep OK )" ; fi

  # Try roma if cartago fails
  if [[ $COPY3 != OK* ]] ; then COPY3="$( $HOME/bin/copy2csa-00.sh $FIL.ccx.bin roma /mnt/ifs/NewsScape/Rosenthal | tail -n2 | grep OK )" ; fi

  # Verify the metadata arrived
  if [[ $COPY3 != OK* ]]
    then echo -e "`date +%F\ %H:%M:%S` \t${SCRIPT%.*} \t$QNUM\t$HOST \tBin copy \tFAILED     \t$FIL.ccx.bin" | tee -a $LOGS/reservations.$( date +%F )
  fi

fi

# EOF
