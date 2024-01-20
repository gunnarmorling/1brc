source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk use java 21.0.1-graal 1>&2

if [ ! -f target/CalculateAverage_pdrakatos_image ]; then
    NATIVE_IMAGE_OPTS="--gc=epsilon -O3 -march=native -R:MaxHeapSize=64m --enable-preview --initialize-at-build-time=dev.morling.onebrc.CalculateAverage_pdrakatos"
    native-image $NATIVE_IMAGE_OPTS -cp target/average-1.0.0-SNAPSHOT.jar -o target/CalculateAverage_pdrakatos_image dev.morling.onebrc.CalculateAverage_pdrakatos
fi