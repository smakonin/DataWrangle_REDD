#!/usr/bin/env python
#
# Importing script to data wrangle (convert, clean, and repair data from) the REDD dataset (Import_REDD_dataset.py)
# Copyright (C) 2013 Stephen Makonin. All Right Reserved.

import sys

redd_dir = '/Volumes/HD-PATU3/REDD/low_freq/house_'
rd_dir = './datasets'
house_ids = [1, 2, 3, 4, 5, 6]
channel_idx = [[]]
load_ids = [[]]

#REDD House 1
#############        0       1       2       3       4       5       6       7       8       9      10
load_ids.append(['MAIN', 'OVEN', 'REFG', 'DISH', 'KTCH', 'LITE', 'DRYR', 'MICR', 'BATH', 'HEAT', 'STOV', 'DIFF'])
#############        1   2   3   4   5   6   7   8   9  10  11  12  13  14  15  16  17  18  19  20
channel_idx.append([ 0,  0,  1,  1,  2,  3,  4,  4,  5,  6,  7,  8,  9, 10,  4,  4,  5,  5,  6,  6])

#REDD House 2
#############        0       1       2       3       4       5       6       7       8
load_ids.append(['MAIN', 'KTCH', 'LITE', 'STOV', 'MICR', 'DRYR', 'REFG', 'DISH', 'GARB', 'DIFF'])
#############        1   2   3   4   5   6   7   8   9  10  11
channel_idx.append([ 0,  0,  1,  2,  3,  4,  5,  1,  6,  7,  8])

#REDD House 3
#############        0       1       2       3       4       5       6       7       8       9      10      11      12
load_ids.append(['MAIN', 'UNKN', 'LITE', 'ELEC', 'REFG', 'GARB', 'DISH', 'FURN', 'DRYR', 'MICR', 'SMOK', 'BATH', 'KTCH', 'DIFF'])
#############        1   2   3   4   5   6   7   8   9  10  11  12  13  14  15  16  17  18  19  20  21  22
channel_idx.append([ 0,  0,  1,  1,  2,  3,  4,  5,  6,  7,  2,  1,  8,  8,  2,  9,  2, 10,  2, 11, 12, 12])

#REDD House 4
#############        0       1       2       3       4       5       6       7       8       9      10      11
load_ids.append(['MAIN', 'LITE', 'FURN', 'KTCH', 'UNKN', 'DRYR', 'STOV', 'AIRC', 'MISC', 'SMOK', 'DISH', 'BATH', 'DIFF'])
#############        1   2   3   4   5   6   7   8   9  10  11  12  13  14  15  16  17  18  19  20
channel_idx.append([ 0,  0,  1,  2,  3,  4,  5,  6,  7,  7,  8,  9,  2,  3, 10, 11, 11,  2,  2,  7])

#REDD House 5
#############        0       1       2       3       4       5       6       7       8       9      10      11      12      13      14
load_ids.append(['MAIN', 'MICR', 'LITE', 'UNKN', 'FURN', 'DRYR', 'SUBP', 'HEAT', 'BATH', 'REFG', 'DISH', 'GARB', 'ELEC', 'KTCH', 'OUTD', 'DIFF'])
#############        1   2   3   4   5   6   7   8   9  10  11  12  13  14  15  16  17  18  19  20  21  22  23  24  25  26
channel_idx.append([ 0,  0,  1,  2,  3,  4,  3,  5,  5,  6,  6,  7,  7,  2,  3,  8,  2,  9,  2, 10, 11, 12,  2, 13, 13, 14])

#REDD House 6
#############        0       1       2       3       4       5       6       7       8       9      10      11
load_ids.append(['MAIN', 'KTCH', 'DRYR', 'STOV', 'ELEC', 'BATH', 'REFG', 'DISH', 'UNKN', 'HEAT', 'LITE', 'AIRC', 'DIFF'])
#############        1   2   3   4   5   6   7   8   9  10  11  12  13  14  15  16  17
channel_idx.append([ 0,  0,  1,  2,  3,  4,  5,  6,  7,  8,  8,  9,  1, 10, 11, 11, 11])


def readfile(filename):
    print 'Reading file:', filename
    fp = open(filename, 'r')    
    f = fp.readlines()
    fp.close()
    ff = [r[:-2].split(' ') for r in f]
    return ff


for house_id in house_ids:
    badval = -999
    dmin = -1
    dmax = -1
    dlen = -1
    d = []
    ch_id = 1
    print 'Processing REDD House', house_id, "..."
    for ch in channel_idx[house_id]:
        ff = readfile('%s%d/channel_%d.dat' % (redd_dir, house_id, ch_id))
        print '\tTimeStamp: dmin =', ff[0][0], ', dmax =', ff[-1][0], ', dlen =', format(int(ff[-1][0]) - int(ff[0][0]) + 1, ',d'), ', actual len =', format(len(ff), ',d')

        if ch_id == 1:
            dmin = int(ff[0][0])
            dmax = int(ff[-1][0])
            dlen = dmax - dmin + 1
            d = [[badval for j in range(len(load_ids[house_id]) + 1)] for i in range(dlen)]
                                
        for i in xrange(len(ff)):
            ts = int(ff[i][0])
            idx = ts - dmin
            #val = float(ff[i][1])
            val = int(float(ff[i][1]))
            
            if idx < 0 or idx > dmax:
                print 'ERROR: out of idx range: idx = %d, channel = %s, ts = %d' % (idx, ch, ts)
                continue
                
            #print d[idx][ch + 1], type(d[idx][ch + 1])
            #print val, type(val)
                
            if d[idx][ch + 1] == badval:
                d[idx][ch + 1] = val
            else:
                d[idx][ch + 1] += val
                
            if ch_id == 1:
                d[idx][0] = ts 
            
        ch_id += 1   
        
    print 'Saving data to CSV file...'
    filename = '%s/REDDhouse%d_lowf_VA.csv' %(rd_dir, house_id)
    fp = open(filename, 'w')
    fp.write( 'TimeStamp , %s\n' % (', '.join(load_ids[house_id])))    
    err_no_ts = 0
    err_incomplete = 0
    err_time_lead = 0
    err_time_lag = 0
    err_neg_noise = 0
    dd = 0
    n1 = 0
    n2 = 0
    for i in xrange(len(d)):
        d[i][-1] = d[i][1] - sum(d[i][2:-1])
        
        if d[i][0] == badval:
            err_no_ts += 1
            continue
        
        if badval in d[i]:
            #print 'ERROR: incomplete:', d[i]
            err_incomplete += 1
            continue
        else:
            if d[i][-1] >= 0:
                n1 = d[i][-1]
        
        
        if d[i][-1] < 0:
            n2 = n1
            for z in range(1, 10):
                if i+z >= len(d):
                    break
                if badval not in d[i+z]:
                    n2 = d[i+z][-1]
            
            corrected = False        
            for z in range(1, 10):
                d1 = d[i-z][1]
                d2 = d[i+z][1]
                dsum = sum(d[i][2:-1])
                diff1 = d1 - dsum
                diff2 = d2 - dsum 
                               
                if diff1 >= 0:
                    d[i][1] = d1
                    d[i][-1] = diff1
                    #print ', d1 =', d1, ', diff1 =', diff1
                    err_time_lag += 1
                    corrected = True
                    break
                elif  diff2 >= 0:
                    d[i][1] = d2
                    d[i][-1] = diff2
                    #print ', d2 =', d2, ', diff1 =', diff2
                    err_time_lead += 1
                    corrected = True
                    break
                                        
            if not corrected:
                print '\tERROR: neg noise:', d[i], ', sum =', sum(d[i][2:-1]), ', new noise =', (n1 + n2) / 2
                d[i][-1] = (n1 + n2) / 2
                d[i][1] = sum(d[i][2:])
                print '\t\tNEW:', d[i]
                err_neg_noise += 1            
                #continue
        
        fp.write('%10d,%s\n' % (d[i][0], ','.join(['%5d' % (d[i][j]) for j in range(1, len(d[i]))])))
        dd += 1

    fp.close()        

    print
    print 'ERRORS: no timestamp =', format(err_no_ts, ',d')
    print 'ERRORS: incomplete rows =', format(err_incomplete, ',d')
    print 'ERRORS: timestamp lead =', format(err_time_lead, ',d')
    print 'ERRORS: timestamp lag =', format(err_time_lag, ',d')
    print 'ERRORS: negative noise =', format(err_neg_noise, ',d')
    print 'DLEN now', dd, 'was', dlen, 'shrunk by', round((1.0 - float(dd) / float(dlen)) * 100.0, 2), '%'
    print
    print 'Data saved to:', filename
    print
	
print
print '...DONE!'    
print
