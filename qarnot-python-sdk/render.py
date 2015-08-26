#! /usr/bin/env python
"""Blender sample"""
from __future__ import print_function
import qapy
import sys
import os
import datetime
import requests.packages.urllib3

def log(line):
    """Log"""
    print("[%s] %s" % (str(datetime.datetime.now()), line))
    sys.stdout.flush()

def main():
    """main"""
    log("Loading config...")
    api = qapy.QApy('qarnot.conf')
    
    # Hide SSL security warnings
    requests.packages.urllib3.disable_warnings()
    
    start_frame = 390 #190
    end_frame = 450 #1921
    batch = 300
    
    while start_frame < end_frame:
        
        log("Creating task...")
        with api.create_task("blender render", "blender", batch) as task:
            for disk in api.disks():
                log("Reusing disk " + disk.description)
                task.resources = disk
                break
            
            log("Sync resources from 'input'")
            task.resources.sync_directory("wordwars", True)
            task.resources.locked = True
            task.resources.commit()
            
            
            log("Setting constants...")
            #available constants:
            #BLEND_ENGINE (CYCLES|BLENDER_RENDER), BLEND_W, BLEND_H, BLEND_RATIO,
            #BLEND_CYCLES_SAMPLES, BLEND_SCENE, BLEND_SLICING, BLEND_FILE,
            #BLEND_FORMAT
            task.constants['BLEND_FILE'] = "wordwars-qarnot.blend"
            task.constants['BLEND_FORMAT'] = "PNG"
            task.constants['BLEND_W'] = 1280
            task.constants['BLEND_H'] = 720
            task.constants['BLEND_ENGINE'] = "BLENDER_RENDER"
            
            if start_frame + batch < end_frame:
                end = start_frame + batch
            else:
                end = end_frame 
            
            task.advanced_range = '[' + str(start_frame) + '-' + str(end ) + ']'
                
            log("Submitting task... "  + task.advanced_range )
            task.submit()

            log("Waiting for task completion...")
            task.wait()

            log("Retrieving results in 'output'")
            if not os.path.exists("output"):
                os.makedirs("output")
            task.download_results("output")
        
        start_frame += batch
        
if __name__ == "__main__":
    main()

