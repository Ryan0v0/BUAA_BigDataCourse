cat ../inputs/1.json | python task1/mapper1.py | sort -t $'\t' -k1,1 | python task1/reducer1.py > task1/out.json

