#!/bin/bash
for file in $(ls 92a0f9d9ced26ad5ccae)
do
    for site_file in $(ls 92a0f9d9ced26ad5ccae/$file)
    do
        for site in $(less $site_file)
        do
            nohup condor_run \"lein trampoline run --discussion-forum --start \\\"http://colorpilot.com/forum/index.php\\\"\" &
        done
    done
done
