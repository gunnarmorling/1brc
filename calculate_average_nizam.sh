#!/bin/sh

# ---------------- CONFIG ----------------

CLASS_NAME=CalculateAverage_Nizam
PACKAGE=dev.nizam
MAIN_CLASS=$PACKAGE.$CLASS_NAME

JAVA_OPTS="--enable-preview --add-modules=jdk.incubator.vector -XX:-TieredCompilation"

# ---------------- BUILD -----------------

echo "Compiling $MAIN_CLASS ..."
./mvnw clean package -DskipTests

if [ $? -ne 0 ]; then
  echo "Build failed"
  exit 1
fi

# ---------------- RUN -------------------

echo "Running $MAIN_CLASS ..."
java $JAVA_OPTS \
  -cp target/average-1.0.0-SNAPSHOT.jar \
  $MAIN_CLASS
