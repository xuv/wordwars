#!/bin/bash

DATE=$(date "+%Y%m%d-%h") # As in 20150627-01 for example
# The magic command to get the path where the script works
SCRIPTPATH=${0%/*}

cd $SCRIPTPATH

BLENDER='blender'

# Relative path to the Blender file
BLENDER_FILE='../qarnot-python-sdk/wordwars/wordwars-qarnot.blend'

echo $DATE
echo $SCRIPTPATH

# Update the Blender file with new content

$BLENDER -b $BLENDER_FILE -P $SCRIPTPATH/fetch.py

# Render the blender file if there was new content (aka. a description file has been created)

DESCRIPTION_FILE=../data/description.txt

if [ -f $DESCRIPTION_FILE ]
then
    echo FOUND $DESCRIPTION_FILE
    DESCRIPTION=$(<$DESCRIPTION_FILE)
    
    # Render local
    # $BLENDER -b ../wordwars.blend -S Prerendered -a
    
    
    # Render using Qarnot SDK 
    cd ../qarnot-python-sdk/
    python3 render.py
    cd ../scripts/
else
	echo NO description.txt FOUND
fi

# Create full video file with pre-rendered clips and frames form Qarnot


# If there was a render, upload it to Youtube and backup the generated files

RENDER_FILE=../render/WordWars0001-2376.avi
TITLE=$(head -n 1 $DESCRIPTION_FILE)

if [ -f $RENDER_FILE ]
then
    echo FOUND render file
    python upload.py --file $RENDER_FILE --title "$TITLE" --description "$DESCRIPTION" --privacyStatus unlisted
    mv $DESCRIPTION_FILE ../data/description-$DATE.txt
    mv $RENDER_FILE ../render/WordWars-$DATE.avi
else
	echo NO render file FOUND
fi
