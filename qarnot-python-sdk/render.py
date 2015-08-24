#! /usr/bin/env python
"""Blender sample"""
from __future__ import print_function
import qapy
import sys
import os
import datetime

def log(line):
    """Log"""
    print("[%s] %s" % (str(datetime.datetime.now()), line))
    sys.stdout.flush()

def main():
    """main"""
    log("Loading config...")
    api = qapy.QApy('qarnot.conf')

    log("Creating task...")
    with api.create_task("blender render", "blender", 1) as task:
        for disk in api.disks():
            log("Reusing disk " + disk.description)
            task.resources = disk
            break

        log("Sync resources from 'input'")
        task.resources.sync_directory("input", True)
        task.resources.locked = True
        task.resources.commit()

        log("Setting constants...")
        #available constants:
        #BLEND_ENGINE (CYCLES|BLENDER_RENDER), BLEND_W, BLEND_H, BLEND_RATIO,
        #BLEND_CYCLES_SAMPLES, BLEND_SCENE, BLEND_SLICING, BLEND_FILE,
        #BLEND_FORMAT
        task.constants['BLEND_FILE'] = "space_corridor_final.blend"
        task.constants['BLEND_FORMAT'] = "PNG"
        task.constants['BLEND_ENGINE'] = "CYCLES"
        task.constants['BLEND_CYCLES_SAMPLES'] = 50

        log("Submitting task...")
        task.submit()

        log("Waiting for task completion...")
        task.wait()

        log("Retrieving results in 'output'")
        if not os.path.exists("output"):
            os.makedirs("output")
        task.download_results("output")

if __name__ == "__main__":
    main()

