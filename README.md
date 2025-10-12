This is a clone to play around with friends after the fact. Scripts and docs are truncated as well as existing submissions to facilitate. 

See [original](https://github.com/gunnarmorling/1brc) for that. 

# Preparation

Assuming Ubuntu. 

```
# Required 3rd party
sudo apt install hyperfine
curl -s "https://get.sdkman.io" | bash
source ~/.sdkman/bin/sdkman-init.sh

# Generate Test Data
sdk use java 21.0.1-open
./mvnw clean verify

# Generates 10M, 100M and 1G test data sets, edit if 100M or 1G is untenable
./create_measurements.sh
```

# Running

```
# Create Own Implementation
./create_fork.sh <username>

# Edit CreateAverage_<username>.java
# Validate 
./mvnw clean verify
./test.sh <username>; echo "Exit code: $?"

# eyeball measure
./evaluate_single.sh <username>

# mesure
./evaluate.sh <username>
```

# Specification

Input file format:

```
Hamburg;12.0
Bulawayo;8.9
Palembang;38.8
```

* `<utf-8 string without ; or \n>;<fraction between --99.9 and 99.9, fractional digit always present>\n`
* up to 10,000 unique station names, 1 to 100 bytes in length
* The rounding of output values must be done using the semantics of IEEE 754 rounding-direction "roundTowardPositive"
* no dependencies

