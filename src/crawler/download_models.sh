for $file in $(ls 92a0f9d9ced26ad5ccae)
do
    for $site in $(ls 92a0f9d9ced26ad5ccae/$file)
    do
        echo \"$site\"
    done
done
