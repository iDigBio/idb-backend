#!/bin/bash
# batches are hard

root=f

for j in a b c d e f 0 1 2 3 4 5 6 7 8 9 ;
do
    for i in a b c d e f 0 1 2 3 4 5 6 7 8 9 ;
    do
        python update_ceph_objects_db.py --bucket idigbio-images-prod-fullsize --prefix "${root}${j}${i}"
    done
done
