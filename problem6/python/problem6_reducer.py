#!/usr/bin/python
import sys

lastKeyI, lastKeyJ = None, None
dictA = {}
dictB = {}
for line in sys.stdin:
    line = line.strip()
    ktmp,vtmp = line.split('\t')
    keyI, keyJ = ktmp.split(' ')
    matrix, identifier, val = vtmp.split(' ')
    if lastKeyI:
        if keyI != lastKeyI or keyJ != lastKeyJ:
            currentVal = 0
            for ident in dictA:
                if ident in dictB:
                    currentVal += dictA[ident] * dictB[ident]
            if currentVal != 0:
                print '%s %s %s' % (lastKeyI, lastKeyJ, currentVal)
            lastKeyI = keyI
            lastKeyJ = keyJ
            dictA = {}
            dictB = {}
        if matrix == "a":
            dictA[identifier] = int(val)
        else:
            dictB[identifier] = int(val)
    else:
        if matrix == "a":
            dictA[identifier] = int(val)
        else:
            dictB[identifier] = int(val)
        lastKeyI = keyI
        lastKeyJ = keyJ
if dictA:
    currentVal = 0
    for ident in dictA:
        if ident in dictB:
            currentVal += dictA[ident] * dictB[ident]
    if currentVal != 0:
        print '%s %s %s' % (lastKeyI, lastKeyJ, currentVal)
