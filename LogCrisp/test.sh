for i in `seq 0 7`; do
	rm variable_${i}_tag
	rm -r variable_$i
	mkdir variable_$i
	./LogCrisp_compression_var/Compressor -I chunks/chunk000$i.txt -O test -T yarn -P $i & 
done
