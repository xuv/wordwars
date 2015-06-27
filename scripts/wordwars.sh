#!/bin/bash

DATE=$(date "+%Y%m%d") # As in 20150627 for example
# The magic command to get the path where the script works
SCRIPTPATH=${0%/*}

BLENDER='blender'

echo $DATE
echo $SCRIPTPATH

# Update the Blender file with new content

$BLENDER -b $SCRIPTPATH/../wordwars.blend -P $SCRIPTPATH/fetch.py

# Render the blender file if there was new content (aka. a description file has been created

DESCRIPTION_FILE=$SCRIPTPATH/../data/description.txt

if [ -f $DESCRIPTION_FILE ]
then
    echo FOUND $DESCRIPTION_FILE
    DESCRIPTION=$(<$DESCRIPTION_FILE)
    $BLENDER -b $SCRIPTPATH/../wordwars.blend -S Prerendered -a
else
	echo NO description.txt FOUND
fi

# If there was a render, upload it to Youtube and backup the generated files

RENDER_FILE=$SCRIPTPATH/../render/WordWars0001-2376.avi
TITLE=$(head -n 1 $DESCRIPTION_FILE)

if [ -f $RENDER_FILE ]
then
    echo FOUND render file
    #python $SCRIPTPATH/upload.py --file $RENDER_FILE --title $TITLE --description $DESCRIPTION --privacyStatus unlisted
    mv $DESCRIPTION_FILE $SCRIPTPATH/../data/description-$DATE.txt
    mv $RENDER_FILE $SCRIPTPATH/../render/WordWars-$DATE.avi
else
	echo NO render file FOUND
fi
