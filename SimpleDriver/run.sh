#
#  sample run command for SimpleDriver

if [ "$1" = "-time" ]; then
	export runTime=${2:-1}
else
	export runTime=1
fi

# Run: java -jar target/nuodb-simple-driver-1.0.0.jar -h to get help

# Run the loader using 10 threads, stop after 1 second (runTime defaults to 1 above). Add -time NN to specify longer.
java -jar target/nuodb-simple-driver-1.0.0.jar -url jdbc:com.nuodb://localhost/testdb -user dba -password dba -threads 10 -time $runTime

