#
# compile the SimpleDriver app
#

export NUODB_HOME=${NUODB_HOME:-/opt/nuodb}

[ -d target ] || mkdir target

javac -cp $NUODB_HOME/jar/nuodbjdbc.jar src/main/java/nuodb/SimpleDriver.java -d target

