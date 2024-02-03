# 1️⃣🐝🏎️ The One Billion Row Challenge

_Status Feb 3: All entries have been evaluated and I am in the process of finalizing the leaderboards. The final leader board will be published by Monday Feb 5._

_Status Feb 1: The challenge has been closed for new submissions. No new pull requests for adding submissions are accepted at this time.
Pending PRs will be evaluated over the next few days._

_Status Jan 31: The challenge will close today at midnight UTC._

_Status Jan 12: As there has been such a large number of entries to this challenge so far (100+), and this is becoming hard to manage, please only create new submissions if you expect them to run in 10 seconds or less on the evaluation machine._

_Status Jan 1: This challenge is [open for submissions](https://www.morling.dev/blog/one-billion-row-challenge/)!_

> **Sponsorship**
>
> A big thank you to my employer [Decodable](https://www.decodable.co/) for funding the evaluation environment and supporting this challenge!

The One Billion Row Challenge (1BRC) is a fun exploration of how far modern Java can be pushed for aggregating one billion rows from a text file.
Grab all your (virtual) threads, reach out to SIMD, optimize your GC, or pull any other trick, and create the fastest implementation for solving this task!

<img src="1brc.png" alt="1BRC" style="display: block; margin-left: auto; margin-right: auto; margin-bottom:1em; width: 50%;">

The text file contains temperature values for a range of weather stations.
Each row is one measurement in the format `<string: station name>;<double: measurement>`, with the measurement value having exactly one fractional digit.
The following shows ten rows as an example:

```
Hamburg;12.0
Bulawayo;8.9
Palembang;38.8
St. John's;15.2
Cracow;12.6
Bridgetown;26.9
Istanbul;6.2
Roseau;34.4
Conakry;31.2
Istanbul;23.0
```

The task is to write a Java program which reads the file, calculates the min, mean, and max temperature value per weather station, and emits the results on stdout like this
(i.e. sorted alphabetically by station name, and the result values per station in the format `<min>/<mean>/<max>`, rounded to one fractional digit):

```
{Abha=-23.0/18.0/59.2, Abidjan=-16.2/26.0/67.3, Abéché=-10.0/29.4/69.0, Accra=-10.1/26.4/66.4, Addis Ababa=-23.7/16.0/67.0, Adelaide=-27.8/17.3/58.5, ...}
```

Submit your implementation by Jan 31 2024 and become part of the leaderboard!

## Results

These are the results from running all entries into the challenge on eight cores of a [Hetzner AX161](https://www.hetzner.com/dedicated-rootserver/ax161) dedicated server (32 core AMD EPYC™ 7502P (Zen2), 128 GB RAM).

| # | Result (m:s.ms) | Implementation     | JDK | Submitter     | Notes     | Certificates |
|---|-----------------|--------------------|-----|---------------|-----------|--------------|
| 1 | 00:01.535 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_thomaswue.java)| 21.0.2-graal | [Thomas Wuerthinger](https://github.com/thomaswue), [Quan Anh Mai](https://github.com/merykitty), [Alfonso² Peterssen](https://github.com/mukel) | GraalVM native binary, uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/thomaswue_merykitty_mukel.pdf) |
| 2 | 00:01.587 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_artsiomkorzun.java)| 21.0.2-graal | [Artsiom Korzun](https://github.com/artsiomkorzun) | GraalVM native binary, uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/artsiomkorzun.pdf) |
| 3 | 00:01.608 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_jerrinot.java)| 21.0.2-graal | [Jaromir Hamala](https://github.com/jerrinot) | GraalVM native binary, uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/jerrinot.pdf) |
|   | 00:01.880 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_serkan_ozal.java)| 21.0.1-open | [Serkan ÖZAL](https://github.com/serkan-ozal) | uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/serkan_ozal.pdf) |
|   | 00:01.921 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_abeobk.java)| 21.0.2-graal | [Van Phu DO](https://github.com/abeobk) | GraalVM native binary, uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/abeobk.pdf) |
|   | 00:02.018 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_stephenvonworley.java)| 21.0.2-graal | [Stephen Von Worley](https://github.com/stephenvonworley) | GraalVM native binary, uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/stephenvonworley.pdf) |
|   | 00:02.157 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_royvanrijn.java)| 21.0.2-graal | [Roy van Rijn](https://github.com/royvanrijn) | GraalVM native binary, uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/royvanrijn.pdf) |
|   | 00:02.319 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_yavuztas.java)| 21.0.2-graal | [Yavuz Tas](https://github.com/yavuztas) | GraalVM native binary, uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/yavuztas.pdf) |
|   | 00:02.332 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_mtopolnik.java)| 21.0.2-graal | [Marko Topolnik](https://github.com/mtopolnik) | GraalVM native binary, uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/mtopolnik.pdf) |
|   | 00:02.367 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_merykittyunsafe.java)| 21.0.1-open | [Quan Anh Mai](https://github.com/merykitty) | uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/merykittyunsafe.pdf) |
|   | 00:02.507 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_gonixunsafe.java)| 21.0.1-open | [gonix](https://github.com/gonix) | uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/gonixunsafe.pdf) |
|   | 00:02.557 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_yourwass.java)| 21.0.1-open | [yourwass](https://github.com/yourwass) | uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/yourwass.pdf) |
|   | 00:02.820 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_linl33.java)| 22.ea.32-open | [Li Lin](https://github.com/linl33) | uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/linl33.pdf) |
|   | 00:02.995 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_tivrfoa.java)| 21.0.2-graal | [tivrfoa](https://github.com/tivrfoa) | GraalVM native binary, uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/tivrfoa.pdf) |
|   | 00:02.997 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_gonix.java)| 21.0.1-open | [gonix](https://github.com/gonix) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/gonix.pdf) |
|   | 00:03.095 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_JamalMulla.java)| 21.0.2-graal | [Jamal Mulla](https://github.com/JamalMulla) | GraalVM native binary, uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/JamalMulla.pdf) |
|   | 00:03.210 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_merykitty.java)| 21.0.1-open | [Quan Anh Mai](https://github.com/merykitty) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/merykitty.pdf) |
|   | 00:03.298 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_vemanaNonIdiomatic.java)| 21.0.1-graal | [Subrahmanyam](https://github.com/vemana) | uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/vemanaNonIdiomatic.pdf) |
|   | 00:03.431 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_roman_r_m.java)| 21.0.1-graal | [Roman Musin](https://github.com/roman-r-m) | GraalVM native binary, uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/roman_r_m.pdf) |
|   | 00:03.469 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_ebarlas.java)| 21.0.2-graal | [Elliot Barlas](https://github.com/ebarlas) | GraalVM native binary, uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/ebarlas.pdf) |
|   | 00:03.698 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_hundredwatt.java)| 21.0.1-graal | [Jason Nochlin](https://github.com/hundredwatt) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/hundredwatt.pdf) |
|   | 00:03.785 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_zerninv.java)| 21.0.2-graal | [zerninv](https://github.com/zerninv) | GraalVM native binary, uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/zerninv.pdf) |
|   | 00:03.820 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_iziamos.java)| 21.0.2-graal | [John Ziamos](https://github.com/iziamos) | GraalVM native binary, uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/iziamos.pdf) |
|   | 00:03.902 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_jparera.java)| 21.0.1-open | [Juan Parera](https://github.com/jparera) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/jparera.pdf) |
|   | 00:03.966 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_jincongho.java)| 21.0.1-open | [Jin Cong Ho](https://github.com/jincongho) | uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/jincongho.pdf) |
|   | 00:03.991 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_vaidhy.java)| 21.0.1-graal | [Vaidhy Mayilrangam](https://github.com/vaidhy) | uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/vaidhy.pdf) |
|   | 00:04.066 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_JesseVanRooy.java)| 21.0.1-open | [JesseVanRooy](https://github.com/JesseVanRooy) | uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/JesseVanRooy.pdf) |
|   | 00:04.101 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_JaimePolidura.java)| 21.0.2-graal | [Jaime Polidura](https://github.com/JaimePolidura) | GraalVM native binary, uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/JaimePolidura.pdf) |
|   | 00:04.209 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_giovannicuccu.java)| 21.0.1-open | [Giovanni Cuccu](https://github.com/giovannicuccu) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/giovannicuccu.pdf) |
|   | 00:04.474 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_gamlerhart.java)| 21.0.1-open | [Roman Stoffel](https://github.com/gamlerhart) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/gamlerhart.pdf) |
|   | 00:04.676 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_plevart.java)| 21.0.2-tem | [Peter Levart](https://github.com/plevart) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/plevart.pdf) |
|   | 00:04.684 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_gigiblender.java)| 21.0.1-open | [Florin Blanaru](https://github.com/gigiblender) | uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/gigiblender.pdf) |
|   | 00:04.701 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_ianopolousfast.java)| 21.0.1-open | [Dr Ian Preston](https://github.com/ianopolousfast) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/ianopolousfast.pdf) |
|   | 00:04.741 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_cliffclick.java)| 21.0.1-open | [Cliff Click](https://github.com/cliffclick) | uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/cliffclick.pdf) |
|   | 00:04.800 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_parkertimmins.java)| 21.0.1-open | [Parker Timmins](https://github.com/parkertimmins) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/parkertimmins.pdf) |
|   | 00:04.884 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_shipilev.java)| 21.0.1-open | [Aleksey Shipilëv](https://github.com/shipilev) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/shipilev.pdf) |
|   | 00:04.920 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_vemana.java)| 21.0.1-graal | [Subrahmanyam](https://github.com/vemana) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/vemana.pdf) |
|   | 00:05.077 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_jonathanaotearoa.java)| 21.0.2-graal | [Jonathan Wright](https://github.com/jonathan-aotearoa) | GraalVM native binary, uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/jonathanaotearoa.pdf) |
|   | 00:05.142 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_arjenw.java)| 21.0.1-open | [Arjen Wisse](https://github.com/arjenw) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/arjenw.pdf) |
|   | 00:05.167 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_melgenek.java)| 21.0.2-open | [Yevhenii Melnyk](https://github.com/melgenek) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/melgenek.pdf) |
|   | 00:05.235 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_unbounded.java)| 21.0.1-open | [unbounded](https://github.com/unbounded) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/unbounded.pdf) |
|   | 00:05.336 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_EduardoSaverin.java)| java | [Sumit Chaudhary](https://github.com/EduardoSaverin) | uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/EduardoSaverin.pdf) |
|   | 00:05.354 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_armandino.java)| 21.0.2-graal | [Arman Sharif](https://github.com/armandino) | GraalVM native binary, uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/armandino.pdf) |
|   | 00:05.478 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_obourgain.java)| 21.0.1-open | [Olivier Bourgain](https://github.com/obourgain) | uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/obourgain.pdf) |
|   | 00:05.559 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_PanagiotisDrakatos.java)| 21.0.1-graal | [Panagiotis Drakatos](https://github.com/PanagiotisDrakatos) | GraalVM native binary | [Certificate](http://gunnarmorling.github.io/1brc-certificates/PanagiotisDrakatos.pdf) |
|   | 00:05.887 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_charlibot.java)| 21.0.1-graal | [Charlie Evans](https://github.com/charlibot) | uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/charlibot.pdf) |
|   | 00:05.979 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_spullara.java)| 21.0.1-graal | [Sam Pullara](https://github.com/spullara) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/spullara.pdf) |
|   | 00:06.166 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_isolgpus.java)| 21.0.1-open | [Jamie Stansfield](https://github.com/isolgpus) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/isolgpus.pdf) |
|   | 00:06.257 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_flippingbits.java)| 21.0.1-graal | [Stefan Sprenger](https://github.com/flippingbits) | uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/flippingbits.pdf) |
|   | 00:06.392 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_dpsoft.java)| 21.0.2-graal | [Diego Parra](https://github.com/dpsoft) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/dpsoft.pdf) |
|   | 00:06.576 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_as-com.java)| 21.0.1-open | [Andrew Sun](https://github.com/as-com) | uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/as-com.pdf) |
|   | 00:06.635 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_justplainlaake.java)| 21.0.1-graal | [Laake Scates-Gervasi](https://github.com/justplainlaake) | GraalVM native binary, uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/justplainlaake.pdf) |
|   | 00:06.654 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_jbachorik.java)| 21.0.1-graal | [Jaroslav Bachorik](https://github.com/jbachorik) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/jbachorik.pdf) |
|   | 00:06.715 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_algirdasrascius.java)| 21.0.1-open | [Algirdas Raščius](https://github.com/algirdasrascius) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/algirdasrascius.pdf) |
|   | 00:06.884 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_rcasteltrione.java)| 21.0.1-graal | [rcasteltrione](https://github.com/rcasteltrione) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/rcasteltrione.pdf) |
|   | 00:06.982 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_ChrisBellew.java)| 21.0.1-open | [Chris Bellew](https://github.com/ChrisBellew) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/ChrisBellew.pdf) |
|   | 00:07.563 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_3j5a.java)| 21.0.1-graal | [3j5a](https://github.com/3j5a) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/3j5a.pdf) |
|   | 00:07.680 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_C5H12O5.java)| 21.0.1-graal | [Xylitol](https://github.com/C5H12O5) | uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/C5H12O5.pdf) |
|   | 00:07.712 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_anitasv.java)| 21.0.1-graal | [Anita SV](https://github.com/anitasv) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/anitasv.pdf) |
|   | 00:07.730 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_jotschi.java)| 21.0.1-open | [Johannes Schüth](https://github.com/jotschi) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/jotschi.pdf) |
|   | 00:07.894 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_tonivade.java)| 21.0.2-tem | [Antonio Muñoz](https://github.com/tonivade) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/tonivade.pdf) |
|   | 00:07.925 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_ricardopieper.java)| 21.0.1-graal | [Ricardo Pieper](https://github.com/ricardopieper) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/ricardopieper.pdf) |
|   | 00:07.948 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_Smoofie.java)| java | [Smoofie](https://github.com/Smoofie) | uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/Smoofie.pdf) |
|   | 00:08.157 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_JurenIvan.java)| 21.0.1-open | [JurenIvan](https://github.com/JurenIvan) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/JurenIvan.pdf) |
|   | 00:08.167 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_ddimtirov.java)| 21.0.1-tem | [Dimitar Dimitrov](https://github.com/ddimtirov) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/ddimtirov.pdf) |
|   | 00:08.214 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_deemkeen.java)| 21.0.1-open | [deemkeen](https://github.com/deemkeen) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/deemkeen.pdf) |
|   | 00:08.255 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_mattiz.java)| 21.0.1-open | [Mathias Bjerke](https://github.com/mattiz) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/mattiz.pdf) |
|   | 00:08.398 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_artpar.java)| 21.0.1-open | [Parth Mudgal](https://github.com/artpar) | uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/artpar.pdf) |
|   | 00:08.489 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_gnabyl.java)| 21.0.1-graal | [Bang NGUYEN](https://github.com/gnabyl) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/gnabyl.pdf) |
|   | 00:08.517 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_ags313.java)| 21.0.1-graal | [ags](https://github.com/ags313) | uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/ags313.pdf) |
|   | 00:08.557 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_adriacabeza.java)| 21.0.1-graal | [Adrià Cabeza](https://github.com/adriacabeza) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/adriacabeza.pdf) |
|   | 00:08.622 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_kuduwa_keshavram.java)| 21.0.1-graal | [Keshavram Kuduwa](https://github.com/kuduwa-keshavram) | uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/kuduwa_keshavram.pdf) |
|   | 00:08.892 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_fatroom.java)| 21.0.1-open | [Roman Romanchuk](https://github.com/fatroom) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/fatroom.pdf) |
|   | 00:08.896 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_anestoruk.java)| 21.0.1-open | [Andrzej Nestoruk](https://github.com/anestoruk) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/anestoruk.pdf) |
|   | 00:09.020 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_yemreinci.java)| 21.0.1-open | [yemreinci](https://github.com/yemreinci) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/yemreinci.pdf) |
|   | 00:09.071 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_gabrielreid.java)| 21.0.1-open | [Gabriel Reid](https://github.com/gabrielreid) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/gabrielreid.pdf) |
|   | 00:09.352 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_filiphr.java)| 21.0.1-graal | [Filip Hrisafov](https://github.com/filiphr) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/filiphr.pdf) |
|   | 00:09.725 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_martin2038.java)| 21.0.2-graal | [Martin](https://github.com/martin2038) | GraalVM native binary | [Certificate](http://gunnarmorling.github.io/1brc-certificates/martin2038.pdf) |
|   | 00:09.867 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_ricardopieper.java)| 21.0.1-graal | [Ricardo Pieper](https://github.com/ricardopieper) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/ricardopieper.pdf) |
|   | 00:09.945 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_japplis.java)| 21.0.1-open | [Anthony Goubard](https://github.com/japplis) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/japplis.pdf) |
|   | 00:10.092 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_phd3.java)| 21.0.1-graal | [Pratham](https://github.com/phd3) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/phd3.pdf) |
|   | 00:10.127 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_artpar.java)| 21.0.1-open | [Parth Mudgal](https://github.com/artpar) | uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/artpar.pdf) |
|   | 00:11.577 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_netrunnereve.java)| 21.0.1-open | [Eve](https://github.com/netrunnereve) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/netrunnereve.pdf) |
|   | 00:10.473 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_raipc.java)| 21.0.1-open | [Anton Rybochkin](https://github.com/raipc) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/raipc.pdf) |
|   | 00:11.119 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_lawrey.java)| 21.0.1-open | [lawrey](https://github.com/lawrey) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/lawrey.pdf) |
|   | 00:11.156 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_YannMoisan.java)| java | [Yann Moisan](https://github.com/YannMoisan) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/YannMoisan.pdf) |
|   | 00:11.167 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_palmr.java)| 21.0.1-open | [Nick Palmer](https://github.com/palmr) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/palmr.pdf) |
|   | 00:11.352 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_karthikeyan97.java)| 21.0.1-open | [karthikeyan97](https://github.com/karthikeyan97) | uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/karthikeyan97.pdf) |
|   | 00:11.363 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_godofwharf.java)| 21.0.2-tem | [Guruprasad Sridharan](https://github.com/godofwharf) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/godofwharf.pdf) |
|   | 00:11.405 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_imrafaelmerino.java)| 21.0.1-graal | [Rafael Merino García](https://github.com/imrafaelmerino) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/imrafaelmerino.pdf) |
|   | 00:11.406 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_gabrielfoo.java)| 21.0.1-graal | [gabrielfoo](https://github.com/gabrielfoo) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/gabrielfoo.pdf) |
|   | 00:11.433 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_jatingala.java)| 21.0.1-graal | [Jatin Gala](https://github.com/jatingala) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/jatingala.pdf) |
|   | 00:11.505 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_bufistov.java)| 21.0.1-open | [Dmitry Bufistov](https://github.com/dmitry-midokura) | uses Unsafe | [Certificate](http://gunnarmorling.github.io/1brc-certificates/bufistov.pdf) |
|   | 00:11.744 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_slovdahl.java)| 21.0.2-tem | [Sebastian Lövdahl](https://github.com/slovdahl) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/slovdahl.pdf) |
|   | 00:11.805 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_coolmineman.java)| 21.0.1-graal | [Cool_Mineman](https://github.com/coolmineman) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/coolmineman.pdf) |
|   | 00:11.934 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_arjenvaneerde.java)| 21.0.1-open | [arjenvaneerde](https://github.com/arjenvaneerde) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/arjenvaneerde.pdf) |
|   | 00:12.220 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_richardstartin.java)| 21.0.1-open | [Richard Startin](https://github.com/richardstartin) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/richardstartin.pdf) |
|   | 00:12.495 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_SamuelYvon.java)| 21.0.1-graal | [Samuel Yvon](https://github.com/SamuelYvon) | GraalVM native binary | [Certificate](http://gunnarmorling.github.io/1brc-certificates/SamuelYvon.pdf) |
|   | 00:12.568 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_MeanderingProgrammer.java)| 21.0.1-graal | [Vlad](https://github.com/MeanderingProgrammer) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/MeanderingProgrammer.pdf) |
|   | 00:12.800 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_yonatang.java)| java | [Yonatan Graber](https://github.com/yonatang) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/yonatang.pdf) |
|   | 00:13.013 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_thanhtrinity.java)| 21.0.1-graal | [Thanh Duong](https://github.com/thanhtrinity) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/thanhtrinity.pdf) |
|   | 00:13.071 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_ianopolous.java)| 21.0.1-open | [Dr Ian Preston](https://github.com/ianopolous) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/ianopolous.pdf) |
|   | 00:13.729 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_cb0s.java)| java | [Cedric Boes](https://github.com/cb0s) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/cb0s.pdf) |
|   | 00:13.817 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_entangled90.java)| 21.0.1-open | [Carlo](https://github.com/entangled90) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/entangled90.pdf) |
|   | 00:14.502 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_eriklumme.java)| 21.0.1-graal | [eriklumme](https://github.com/eriklumme) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/eriklumme.pdf) |
|   | 00:14.772 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_kevinmcmurtrie.java)| 21.0.1-open | [Kevin McMurtrie](https://github.com/kevinmcmurtrie) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/kevinmcmurtrie.pdf) |
|   | 00:14.867 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_berry120.java)| 21.0.1-open | [Michael Berry](https://github.com/berry120) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/berry120.pdf) |
|   | 00:14.900 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_Judekeyser.java)| java | [Judekeyser](https://github.com/Judekeyser) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/Judekeyser.pdf) |
|   | 00:15.006 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_PawelAdamski.java)| java | [Paweł Adamski](https://github.com/PawelAdamski) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/PawelAdamski.pdf) |
|   | 00:15.662 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_semotpan.java)| 21.0.1-open | [Serghei Motpan](https://github.com/semotpan) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/semotpan.pdf) |
|   | 00:16.063 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_makohn.java)| 21.0.1-open | [Marek Kohn](https://github.com/makohn) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/makohn.pdf) |
|   | 00:16.457 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_bytesfellow.java)| 21.0.1-open | [Aleksei](https://github.com/bytesfellow) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/bytesfellow.pdf) |
|   | 00:16.953 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_gauravdeshmukh.java)| 21.0.1-open | [Gaurav Anantrao Deshmukh](https://github.com/gauravdeshmukh) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/gauravdeshmukh.pdf) |
|   | 00:17.046 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_dkarampi.java)| 21.0.1-open | [Dimitris Karampinas](https://github.com/dkarampi) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/dkarampi.pdf) |
|   | 00:17.086 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_breejesh.java)| java | [Breejesh Rathod](https://github.com/breejesh) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/breejesh.pdf) |
|   | 00:17.490 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_kgeri.java)| 21.0.1-open | [Gergely Kiss](https://github.com/kgeri) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/kgeri.pdf) |
|   | 00:17.255 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_tkosachev.java)| 21.0.1-open | [tkosachev](https://github.com/tkosachev) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/tkosachev.pdf) |
|   | 00:17.520 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_faridtmammadov.java)| 21.0.1-open | [Farid](https://github.com/faridtmammadov) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/faridtmammadov.pdf) |
|   | 00:17.717 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_omarchenko4j.java)| 21.0.1-open | [Oleh Marchenko](https://github.com/omarchenko4j) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/omarchenko4j.pdf) |
|   | 00:17.815 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_hallvard.java)| 21.0.1-open | [Hallvard Trætteberg](https://github.com/hallvard) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/hallvard.pdf) |
|   | 00:17.932 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_plbpietrz.java)| 21.0.1-open | [Bartłomiej Pietrzyk](https://github.com/plbpietrz) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/plbpietrz.pdf) |
|   | 00:18.251 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_seijikun.java)| 21.0.1-graal | [Markus Ebner](https://github.com/seijikun) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/seijikun.pdf) |
|   | 00:18.448 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_moysesb.java)| 21.0.1-open | [Moysés Borges Furtado](https://github.com/moysesb) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/moysesb.pdf) |
|   | 00:18.771 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_davecom.java)| 21.0.1-graal | [David Kopec](https://github.com/davecom) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/davecom.pdf) |
|   | 00:18.902 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_maximz101.java)| 21.0.1-graal | [Maxime](https://github.com/maximz101) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/maximz101.pdf) |
|   | 00:19.357 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_truelive.java)| 21.0.1-graalce | [Roman Schweitzer](https://github.com/truelive) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/truelive.pdf) |
|   | 00:20.691 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_Kidlike.java)| 21.0.1-graal | [Kidlike](https://github.com/Kidlike) | GraalVM native binary | [Certificate](http://gunnarmorling.github.io/1brc-certificates/Kidlike.pdf) |
|   | 00:21.989 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_couragelee.java)| 21.0.1-open | [couragelee](https://github.com/couragelee) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/couragelee.pdf) |
|   | 00:22.188 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_jgrateron.java)| 21.0.1-open | [Jairo Graterón](https://github.com/jgrateron) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/jgrateron.pdf) |
|   | 00:22.334 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_albertoventurini.java)| 21.0.1-open | [Alberto Venturini](https://github.com/albertoventurini) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/albertoventurini.pdf) |
|   | 00:22.457 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_rby.java)| 21.0.1-open | [Ramzi Ben Yahya](https://github.com/rby) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/rby.pdf) |
|   | 00:22.471 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_0xshivamagarwal.java)| 21.0.1-open | [Shivam Agarwal](https://github.com/0xshivamagarwal) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/0xshivamagarwal.pdf) |
|   | 00:24.986 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_kumarsaurav123.java)| 21.0.1-open | [kumarsaurav123](https://github.com/kumarsaurav123) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/kumarsaurav123.pdf) |
|   | 00:25.064 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_sudhirtumati.java)| 21.0.2-open | [Sudhir Tumati](https://github.com/sudhirtumati) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/sudhirtumati.pdf) |
|   | 00:26.500 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_felix19350.java)| 21.0.1-open | [Bruno Félix](https://github.com/felix19350) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/felix19350.pdf) |
|   | 00:28.381 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_bjhara.java)| 21.0.1-open | [Hampus](https://github.com/bjhara) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/bjhara.pdf) |
|   | 00:29.741 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_xpmatteo.java)| 21.0.1-open | [Matteo Vaccari](https://github.com/xpmatteo) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/xpmatteo.pdf) |
|   | 00:32.018 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_padreati.java)| 21.0.1-open | [Aurelian Tutuianu](https://github.com/padreati) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/padreati.pdf) |
|   | 00:34.388 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_twobiers.java)| 21.0.1-tem | [Tobi](https://github.com/twobiers) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/twobiers.pdf) |
|   | 00:35.875 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_MahmoudFawzyKhalil.java)| 21.0.1-open | [MahmoudFawzyKhalil](https://github.com/MahmoudFawzyKhalil) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/MahmoudFawzyKhalil.pdf) |
|   | 00:36.180 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_hchiorean.java)| 21.0.1-open | [Horia Chiorean](https://github.com/hchiorean) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/hchiorean.pdf) |
|   | 00:36.424 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_manishgarg90.java)| java | [Manish Garg](https://github.com/manishgarg90) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/manishgarg90.pdf) |
|   | 00:38.340 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_AbstractKamen.java)| 21.0.1-open | [AbstractKamen](https://github.com/AbstractKamen) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/AbstractKamen.pdf) |
|   | 00:41.982 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_criccomini.java)| 21.0.1-open | [Chris Riccomini](https://github.com/criccomini) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/criccomini.pdf) |
|   | 00:42.893 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_javamak.java)| 21.0.1-open | [javamak](https://github.com/javamak) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/javamak.pdf) |
|   | 00:46.597 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_maeda6uiui.java)| 21.0.1-open | [Maeda-san](https://github.com/maeda6uiui) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/maeda6uiui.pdf) |
|   | 00:58.811 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_Ujjwalbharti.java)| 21.0.1-open | [Ujjwal Bharti](https://github.com/Ujjwalbharti) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/Ujjwalbharti.pdf) |
|   | 01:05.094 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_muditsaxena.java)| 21.0.1-open | [Mudit Saxena](https://github.com/mudit-saxena) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/muditsaxena.pdf) |
|   | 01:05.979 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_dqhieuu.java)| 21.0.1-graal | [Hieu Dao Quang](https://github.com/dqhieuu) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/dqhieuu.pdf) |
|   | 01:06.790 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_khmarbaise.java)| 21.0.1-open | [Karl Heinz Marbaise](https://github.com/khmarbaise) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/khmarbaise.pdf) |
|   | 01:06.944 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_santanu.java)| 21.0.1-open | [santanu](https://github.com/santanu) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/santanu.pdf) |
|   | 01:07.014 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_pedestrianlove.java)| 21.0.1-open | [pedestrianlove](https://github.com/pedestrianlove) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/pedestrianlove.pdf) |
|   | 01:07.101 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_jeevjyot.java)| 21.0.1-open | [Jeevjyot Singh Chhabda](https://github.com/jeevjyot) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/jeevjyot.pdf) |
|   | 01:08.811 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_alesj.java)| 21.0.1-open | [Aleš Justin](https://github.com/alesj) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/alesj.pdf) |
|   | 01:08.908 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_itaske.java)| 21.0.1-open | [itaske](https://github.com/itaske) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/itaske.pdf) |
|   | 01:09.595 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_agoncal.java)| 21.0.1-tem | [Antonio Goncalves](https://github.com/agoncal) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/agoncal.pdf) |
|   | 01:09.882 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_rprabhu.java)| 21.0.1-open | [Prabhu R](https://github.com/rprabhu) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/rprabhu.pdf) |
|   | 01:14.815 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_anandmattikopp.java)| 21.0.1-open | [twohardthings](https://github.com/anandmattikopp) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/anandmattikopp.pdf) |
|   | 01:25.801 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_ivanklaric.java)| 21.0.1-open | [ivanklaric](https://github.com/ivanklaric) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/ivanklaric.pdf) |
|   | 01:33.594 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_gnmathur.java)| 21.0.1-open | [Gaurav Mathur](https://github.com/gnmathur) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/gnmathur.pdf) |
|   | 01:53.208 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_mahadev_k.java)| java | [Mahadev K](https://github.com/mahadev-k) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/mahadev_k.pdf) |
|   | 01:56.607 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_abfrmblr.java)| 21.0.1-open | [Abhilash](https://github.com/abfrmblr) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/abfrmblr.pdf) |
|   | 03:43.521 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_yehwankim23.java)| 21.0.1-open | [김예환 Ye-Hwan Kim (Sam)](https://github.com/yehwankim23) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/yehwankim23.pdf) |
|   | 03:59.760 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_fragmede.java)| 21.0.1-open | [Samson](https://github.com/fragmede) |  | [Certificate](http://gunnarmorling.github.io/1brc-certificates/fragmede.pdf) |
|   | ---       | | | | |
|   | 04:49.679 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_baseline.java) (Baseline) | 21.0.1-open | [Gunnar Morling](https://github.com/gunnarmorling) |  |

Note that I am not super-scientific in the way I'm running the contenders
(see [Evaluating Results](#evaluating-results) for the details).
This is not a high-fidelity micro-benchmark and there can be variations of up to +-3% between runs.
So don't be too hung up on the exact ordering of your entry compared to others in close proximity.
The primary purpose of this challenge is to learn something new, have fun along the way, and inspire others to do the same.
The leaderboard is only means to an end for achieving this goal.
If you observe drastically different results though, please open an issue.

See [Entering the Challenge](#entering-the-challenge) for instructions how to enter the challenge with your own implementation.
The [Show & Tell](https://github.com/gunnarmorling/1brc/discussions/categories/show-and-tell) features a wide range of 1BRC entries built using other languages, databases, and tools.

### Bonus Results

This section lists results from running the fastest N entries with different configurations.
As entries have been optimized towards the specific conditions of the original challenge description and set-up
(such as size of the key set),
challenge entries may perform very differently across different configurations.
These bonus results are provided here for informational purposes only.
For the 1BRC challenge, only the results in the previous section are of importance.

#### 32 Cores / 64 Threads

For officially evaluating entries into the challenge, each contender is run on eight cores of the evaluation machine (AMD EPYC™ 7502P).
Here are the results from running the top 50 entries (as of commit [e1fb378a](https://github.com/gunnarmorling/1brc/commit/e1fb378acce53d8c3035ee4813ae377aaf51aa3c), Feb 2) on all 32 cores / 64 threads (i.e. SMT is enabled) of the machine:

| # | Result (m:s.ms) | Implementation     | JDK | Submitter     | Notes     |
|---|-----------------|--------------------|-----|---------------|-----------|
| 1 | 00:00.323 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_jerrinot.java)| 21.0.2-graal | [Jaromir Hamala](https://github.com/jerrinot) | GraalVM native binary, uses Unsafe |
| 2 | 00:00.326 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_thomaswue.java)| 21.0.2-graal | [Thomas Wuerthinger](https://github.com/thomaswue), [Quan Anh Mai](https://github.com/merykitty), [Alfonso² Peterssen](https://github.com/mukel) | GraalVM native binary, uses Unsafe |
| 3 | 00:00.349 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_artsiomkorzun.java)| 21.0.2-graal | [Artsiom Korzun](https://github.com/artsiomkorzun) | GraalVM native binary, uses Unsafe |
|   | 00:00.351 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_abeobk.java)| 21.0.2-graal | [Van Phu DO](https://github.com/abeobk) | GraalVM native binary, uses Unsafe |
|   | 00:00.389 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_stephenvonworley.java)| 21.0.2-graal | [Stephen Von Worley](https://github.com/stephenvonworley) | GraalVM native binary, uses Unsafe |
|   | 00:00.408 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_yavuztas.java)| 21.0.2-graal | [Yavuz Tas](https://github.com/yavuztas) | GraalVM native binary, uses Unsafe |
|   | 00:00.415 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_royvanrijn.java)| 21.0.2-graal | [Roy van Rijn](https://github.com/royvanrijn) | GraalVM native binary, uses Unsafe |
|   | 00:00.499 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_mtopolnik.java)| 21.0.2-graal | [Marko Topolnik](https://github.com/mtopolnik) | GraalVM native binary, uses Unsafe |
|   | 00:00.602 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_roman_r_m.java)| 21.0.1-graal | [Roman Musin](https://github.com/roman-r-m) | GraalVM native binary, uses Unsafe |
|   | 00:00.623 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_gonixunsafe.java)| 21.0.1-open | [gonix](https://github.com/gonixunsafe) | uses Unsafe |
|   | 00:00.710 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_JamalMulla.java)| 21.0.2-graal | [Jamal Mulla](https://github.com/JamalMulla) | GraalVM native binary, uses Unsafe |
|   | 00:00.727 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_tivrfoa.java)| 21.0.2-graal | [tivrfoa](https://github.com/tivrfoa) | GraalVM native binary, uses Unsafe |
|   | 00:00.774 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_serkan_ozal.java)| 21.0.1-open | [Serkan ÖZAL](https://github.com/serkan-ozal) | uses Unsafe |
|   | 00:00.788 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_ebarlas.java)| 21.0.2-graal | [Elliot Barlas](https://github.com/ebarlas) | GraalVM native binary, uses Unsafe |
|   | 00:00.832 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_zerninv.java)| 21.0.2-graal | [zerninv](https://github.com/zerninv) | GraalVM native binary, uses Unsafe |
|   | 00:00.840 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_gonix.java)| 21.0.1-open | [gonix](https://github.com/gonix) |  |
|   | 00:00.857 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_JaimePolidura.java)| 21.0.2-graal | [Jaime Polidura](https://github.com/JaimePolidura) | GraalVM native binary, uses Unsafe |
|   | 00:00.880 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_iziamos.java)| 21.0.2-graal | [John Ziamos](https://github.com/iziamos) | GraalVM native binary, uses Unsafe |
|   | 00:00.939 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_shipilev.java)| 21.0.1-open | [Aleksey Shipilëv](https://github.com/shipilev) |  |
|   | 00:01.026 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_JesseVanRooy.java)| 21.0.1-open | [JesseVanRooy](https://github.com/JesseVanRooy) | uses Unsafe |
|   | 00:01.118 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_jonathanaotearoa.java)| 21.0.2-graal | [Jonathan Wright](https://github.com/jonathan-aotearoa) | GraalVM native binary |
|   | 00:01.140 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_armandino.java)| 21.0.2-graal | [Arman Sharif](https://github.com/armandino) | GraalVM native binary, uses Unsafe |
|   | 00:01.143 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_cliffclick.java)| 21.0.1-open | [Cliff Click](https://github.com/cliffclick) | uses Unsafe |
|   | 00:01.169 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_melgenek.java)| 21.0.2-open | [Yevhenii Melnyk](https://github.com/melgenek) |  |
|   | 00:01.188 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_vemanaNonIdiomatic.java)| 21.0.1-graal | [Subrahmanyam](https://github.com/vemanaNonIdiomatic) | uses Unsafe |
|   | 00:01.193 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_gigiblender.java)| 21.0.1-open | [Florin Blanaru](https://github.com/gigiblender) | uses Unsafe |
|   | 00:01.234 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_obourgain.java)| 21.0.1-open | [Olivier Bourgain](https://github.com/obourgain) | uses Unsafe |
|   | 00:01.242 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_merykittyunsafe.java)| 21.0.1-open | [Quan Anh Mai](https://github.com/merykittyunsafe) | uses Unsafe |
|   | 00:01.252 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_jincongho.java)| 21.0.1-open | [Jin Cong Ho](https://github.com/jincongho) | uses Unsafe |
|   | 00:01.267 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_linl33.java)| 22.ea.32-open | [Li Lin](https://github.com/linl33) | uses Unsafe |
|   | 00:01.363 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_plevart.java)| 21.0.2-tem | [Peter Levart](https://github.com/plevart) |  |
|   | 00:01.380 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_hundredwatt.java)| 21.0.1-graal | [Jason Nochlin](https://github.com/hundredwatt) |  |
|   | 00:01.391 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_merykitty.java)| 21.0.1-open | [Quan Anh Mai](https://github.com/merykitty) |  |
|   | 00:01.439 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_arjenw.java)| 21.0.1-open | [Arjen Wisse](https://github.com/arjenw) |  |
|   | 00:01.446 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_ianopolousfast.java)| 21.0.1-open | [Dr Ian Preston](https://github.com/ianopolousfast) |  |
|   | 00:01.504 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_isolgpus.java)| 21.0.1-open | [Jamie Stansfield](https://github.com/isolgpus) |  |
|   | 00:01.514 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_vemana.java)| 21.0.1-graal | [Subrahmanyam](https://github.com/vemana) |  |
|   | 00:01.516 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_vaidhy.java)| 21.0.1-graal | [Vaidhy Mayilrangam](https://github.com/vaidhy) | uses Unsafe |
|   | 00:01.586 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_yourwass.java)| 21.0.1-open | [yourwass](https://github.com/yourwass) | uses Unsafe |
|   | 00:01.647 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_dpsoft.java)| 21.0.2-graal | [Diego Parra](https://github.com/dpsoft) |  |
|   | 00:01.694 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_parkertimmins.java)| 21.0.1-open | [Parker Timmins](https://github.com/parkertimmins) |  |
|   | 00:01.694 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_charlibot.java)| 21.0.1-graal | [Charlie Evans](https://github.com/charlibot) | uses Unsafe |
|   | 00:01.702 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_spullara.java)| 21.0.1-graal | [Sam Pullara](https://github.com/spullara) |  |
|   | 00:01.733 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_EduardoSaverin.java)| java | [Sumit Chaudhary](https://github.com/EduardoSaverin) | uses Unsafe |
|   | 00:01.742 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_unbounded.java)| 21.0.1-open | [unbounded](https://github.com/unbounded) |  |
|   | 00:02.241 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_flippingbits.java)| 21.0.1-graal | [Stefan Sprenger](https://github.com/flippingbits) | uses Unsafe |
|   | 00:02.294 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_giovannicuccu.java)| 21.0.1-open | [Giovanni Cuccu](https://github.com/giovannicuccu) |  |
|   | 00:02.990 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_PanagiotisDrakatos.java)| 21.0.1-graal | [Panagiotis Drakatos](https://github.com/PanagiotisDrakatos) | GraalVM native binary |
|   | 00:03.205 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_jparera.java)| 21.0.1-open | [Juan Parera](https://github.com/jparera) |  |
|   | 00:10.929 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_gamlerhart.java)| 21.0.1-open | [Roman Stoffel](https://github.com/gamlerhart) |  |

#### 10K Key Set

The 1BRC challenge data set contains 413 distinct weather stations, whereas the rules allow for 10,000 different station names to occur.
Here are the results from running the top 40 entries (as of commit [e1fb378a](https://github.com/gunnarmorling/1brc/commit/e1fb378acce53d8c3035ee4813ae377aaf51aa3c), Feb 2) against 1,000,000,000 measurement values across 10K stations (created via _./create_measurements3.sh 1000000000_),
using eight cores on the evaluation machine:

| # | Result (m:s.ms) | Implementation     | JDK | Submitter     | Notes     |
|---|-----------------|--------------------|-----|---------------|-----------|
| 1 | 00:02.957 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_artsiomkorzun.java)| 21.0.2-graal | [Artsiom Korzun](https://github.com/artsiomkorzun) | GraalVM native binary, uses Unsafe |
| 2 | 00:03.058 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_mtopolnik.java)| 21.0.2-graal | [Marko Topolnik](https://github.com/mtopolnik) | GraalVM native binary, uses Unsafe |
| 3 | 00:03.186 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_stephenvonworley.java)| 21.0.2-graal | [Stephen Von Worley](https://github.com/stephenvonworley) | GraalVM native binary, uses Unsafe |
|   | 00:03.998 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_royvanrijn.java)| 21.0.2-graal | [Roy van Rijn](https://github.com/royvanrijn) | GraalVM native binary, uses Unsafe |
|   | 00:04.042 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_jerrinot.java)| 21.0.2-graal | [Jaromir Hamala](https://github.com/jerrinot) | GraalVM native binary, uses Unsafe |
|   | 00:04.289 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_gonixunsafe.java)| 21.0.1-open | [gonix](https://github.com/gonixunsafe) | uses Unsafe |
|   | 00:04.522 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_tivrfoa.java)| 21.0.2-graal | [tivrfoa](https://github.com/tivrfoa) | GraalVM native binary, uses Unsafe |
|   | 00:04.653 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_JamalMulla.java)| 21.0.2-graal | [Jamal Mulla](https://github.com/JamalMulla) | GraalVM native binary, uses Unsafe |
|   | 00:04.733 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_gonix.java)| 21.0.1-open | [gonix](https://github.com/gonix) |  |
|   | 00:04.836 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_vemanaNonIdiomatic.java)| 21.0.1-graal | [Subrahmanyam](https://github.com/vemanaNonIdiomatic) | uses Unsafe |
|   | 00:04.870 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_thomaswue.java)| 21.0.2-graal | [Thomas Wuerthinger](https://github.com/thomaswue), [Quan Anh Mai](https://github.com/merykitty), [Alfonso² Peterssen](https://github.com/mukel) | GraalVM native binary, uses Unsafe |
|   | 00:05.240 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_zerninv.java)| 21.0.2-graal | [zerninv](https://github.com/zerninv) | GraalVM native binary, uses Unsafe |
|   | 00:05.394 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_yavuztas.java)| 21.0.2-graal | [Yavuz Tas](https://github.com/yavuztas) | GraalVM native binary, uses Unsafe |
|   | 00:05.906 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_ebarlas.java)| 21.0.2-graal | [Elliot Barlas](https://github.com/ebarlas) | GraalVM native binary, uses Unsafe |
|   | 00:06.086 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_abeobk.java)| 21.0.2-graal | [Van Phu DO](https://github.com/abeobk) | GraalVM native binary, uses Unsafe |
|   | 00:06.379 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_iziamos.java)| 21.0.2-graal | [John Ziamos](https://github.com/iziamos) | GraalVM native binary, uses Unsafe |
|   | 00:07.113 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_melgenek.java)| 21.0.2-open | [Yevhenii Melnyk](https://github.com/melgenek) |  |
|   | 00:07.542 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_jonathan-aotearoa.java)| 21.0.2-graal | [Jonathan Wright](https://github.com/jonathan-aotearoa) | GraalVM native binary |
|   | 00:07.889 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_gigiblender.java)| 21.0.1-open | [Florin Blanaru](https://github.com/gigiblender) | uses Unsafe |
|   | 00:07.970 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_cliffclick.java)| 21.0.1-open | [Cliff Click](https://github.com/cliffclick) | uses Unsafe |
|   | 00:08.857 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_serkan-ozal.java)| 21.0.1-open | [Serkan ÖZAL](https://github.com/serkan-ozal) |  |
|   | 00:09.333 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_yourwass.java)| 21.0.1-open | [yourwass](https://github.com/yourwass) | uses Unsafe |
|   | 00:09.722 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_shipilev.java)| 21.0.1-open | [Aleksey Shipilëv](https://github.com/shipilev) |  |
|   | 00:09.777 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_vaidhy.java)| 21.0.1-graal | [Vaidhy Mayilrangam](https://github.com/vaidhy) | uses Unsafe |
|   | 00:10.263 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_merykittyunsafe.java)| 21.0.1-open | [Quan Anh Mai](https://github.com/merykittyunsafe) | uses Unsafe |
|   | 00:11.154 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_parkertimmins.java)| 21.0.1-open | [Parker Timmins](https://github.com/parkertimmins) |  |
|   | 00:13.175 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_merykitty.java)| 21.0.1-open | [Quan Anh Mai](https://github.com/merykitty) |  |
|   | 00:13.245 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_ianopolousfast.java)| 21.0.1-open | [Dr Ian Preston](https://github.com/ianopolousfast) |  |
|   | 00:13.377 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_giovannicuccu.java)| 21.0.1-open | [Giovanni Cuccu](https://github.com/giovannicuccu) |  |
|   | 00:13.761 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_jparera.java)| 21.0.1-open | [Juan Parera](https://github.com/jparera) |  |
|   | 00:14.441 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_plevart.java)| 21.0.2-tem | [Peter Levart](https://github.com/plevart) |  |
|   | 00:15.548 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_jincongho.java)| 21.0.1-open | [Jin Cong Ho](https://github.com/jincongho) | uses Unsafe |
|   | 00:17.906 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_hundredwatt.java)| 21.0.1-graal | [Jason Nochlin](https://github.com/hundredwatt) |  |
|   | 00:18.770 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_linl33.java)| 22.ea.32-open | [Li Lin](https://github.com/linl33) | uses Unsafe |
|   | 00:19.106 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_gamlerhart.java)| 21.0.1-open | [Roman Stoffel](https://github.com/gamlerhart) |  |
|   | 00:20.151 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_roman_r_m.java)| 21.0.1-graal | [Roman Musin](https://github.com/roman-r-m) | GraalVM native binary, uses Unsafe; seg-faults occassionally |
|   | 00:22.953 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_JaimePolidura.java)| 21.0.2-graal | [Jaime Polidura](https://github.com/JaimePolidura) | GraalVM native binary, uses Unsafe |
|   | ---       | | | | |
|   | DNF       | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_JesseVanRooy.java)| 21.0.1-open | [JesseVanRooy](https://github.com/JesseVanRooy) | Incorrect output |
|   | DNF       | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_vemana.java)| 21.0.1-graal | [Subrahmanyam](https://github.com/vemana) | Doesn't complete in 60 sec |
|   | DNF       | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_arjenw.java)| 21.0.1-open | [Arjen Wisse](https://github.com/arjenw) | Incorrect output |

## Prerequisites

[Java 21](https://openjdk.org/projects/jdk/21/) must be installed on your system.

## Running the Challenge

This repository contains two programs:

* `dev.morling.onebrc.CreateMeasurements` (invoked via _create\_measurements.sh_): Creates the file _measurements.txt_ in the root directory of this project with a configurable number of random measurement values
* `dev.morling.onebrc.CalculateAverage` (invoked via _calculate\_average\_baseline.sh_): Calculates the average values for the file _measurements.txt_

Execute the following steps to run the challenge:

1. Build the project using Apache Maven:

    ```
    ./mvnw clean verify
    ```

2. Create the measurements file with 1B rows (just once):

    ```
    ./create_measurements.sh 1000000000
    ```

    This will take a few minutes.
    **Attention:** the generated file has a size of approx. **12 GB**, so make sure to have enough diskspace.

    If you're running the challenge with a non-Java language, there's a non-authoritative Python script to generate the measurements file at `src/main/python/create_measurements.py`. The authoritative method for generating the measurements is the Java program `dev.morling.onebrc.CreateMeasurements`.

3. Calculate the average measurement values:

    ```
    ./calculate_average_baseline.sh
    ```

    The provided naive example implementation uses the Java streams API for processing the file and completes the task in ~2 min on environment used for [result evaluation](#evaluating-results).
    It serves as the base line for comparing your own implementation.

4. Optimize the heck out of it:

    Adjust the `CalculateAverage` program to speed it up, in any way you see fit (just sticking to a few rules described below).
    Options include parallelizing the computation, using the (incubating) Vector API, memory-mapping different sections of the file concurrently, using AppCDS, GraalVM, CRaC, etc. for speeding up the application start-up, choosing and tuning the garbage collector, and much more.

## Flamegraph/Profiling

A tip is that if you have [jbang](https://jbang.dev) installed, you can get a flamegraph of your program by running
[async-profiler](https://github.com/jvm-profiling-tools/async-profiler) via [ap-loader](https://github.com/jvm-profiling-tools/ap-loader):

`jbang --javaagent=ap-loader@jvm-profiling-tools/ap-loader=start,event=cpu,file=profile.html -m dev.morling.onebrc.CalculateAverage_yourname target/average-1.0.0-SNAPSHOT.jar`

or directly on the .java file:

`jbang --javaagent=ap-loader@jvm-profiling-tools/ap-loader=start,event=cpu,file=profile.html src/main/java/dev/morling/onebrc/CalculateAverage_yourname`

When you run this, it will generate a flamegraph in profile.html. You can then open this in a browser and see where your program is spending its time.

## Rules and limits

* Any of these Java distributions may be used:
    * Any builds provided by [SDKMan](https://sdkman.io/jdks)
    * Early access builds available on openjdk.net may be used (including EA builds for OpenJDK projects like Valhalla)
    * Builds on [builds.shipilev.net](https://builds.shipilev.net/openjdk-jdk-lilliput/)
If you want to use a build not available via these channels, reach out to discuss whether it can be considered.
* No external library dependencies may be used
* Implementations must be provided as a single source file
* The computation must happen at application _runtime_, i.e. you cannot process the measurements file at _build time_
(for instance, when using GraalVM) and just bake the result into the binary
* Input value ranges are as follows:
    * Station name: non null UTF-8 string of min length 1 character and max length 100 bytes, containing neither `;` nor `\n` characters. (i.e. this could be 100 one-byte characters, or 50 two-byte characters, etc.)
    * Temperature value: non null double between -99.9 (inclusive) and 99.9 (inclusive), always with one fractional digit
* There is a maximum of 10,000 unique station names
* Line endings in the file are `\n` characters on all platforms
* Implementations must not rely on specifics of a given data set, e.g. any valid station name as per the constraints above and any data distribution (number of measurements per station) must be supported
* The rounding of output values must be done using the semantics of IEEE 754 rounding-direction "roundTowardPositive"

## Entering the Challenge

To submit your own implementation to 1BRC, follow these steps:

* Create a fork of the [onebrc](https://github.com/gunnarmorling/onebrc/) GitHub repository.
* Run `./create_fork.sh <your_GH_user>` to copy the baseline implementation to your personal files, or do this manually:
  * Create a copy of _CalculateAverage\_baseline.java_, named _CalculateAverage\_<your_GH_user>.java_, e.g. _CalculateAverage\_doloreswilson.java_.
  * Create a copy of _calculate\_average\_baseline.sh_, named _calculate\_average\_<your_GH_user>.sh_, e.g. _calculate\_average\_doloreswilson.sh_.
  * Adjust that script so that it references your implementation class name. If needed, provide any JVM arguments via the `JAVA_OPTS` variable in that script.
    Make sure that script does not write anything to standard output other than calculation results.
  * (Optional) OpenJDK 21 is used by default. If a custom JDK build is required, create a copy of _prepare\_baseline.sh_, named _prepare\_<your_GH_user>.sh_, e.g. _prepare\_doloreswilson.sh_. Include the SDKMAN command `sdk use java [version]` in the your prepare script.
  * (Optional) If you'd like to use native binaries (GraalVM), add all the required build logic to your _prepare\_<your_GH_user>.sh_ script.
* Make that implementation fast. Really fast.
* Run the test suite by executing _/test.sh <your_GH_user>_; if any differences are reported, fix them before submitting your implementation.
* Create a pull request against the upstream repository, clearly stating
  * The name of your implementation class.
  * The execution time of the program on your system and specs of the same (CPU, number of cores, RAM). This is for informative purposes only, the official runtime will be determined as described below.
* I will run the program and determine its performance as described in the next section, and enter the result to the scoreboard.

**Note:** I reserve the right to not evaluate specific submissions if I feel doubtful about the implementation (I.e. I won't run your Bitcoin miner ;).

If you'd like to discuss any potential ideas for implementing 1BRC with the community,
you can use the [GitHub Discussions](https://github.com/gunnarmorling/onebrc/discussions) of this repository.
Please keep it friendly and civil.

The challenge runs until Jan 31 2024.
Any submissions (i.e. pull requests) created after Jan 31 2024 23:59 UTC will not be considered.

## Evaluating Results

Results are determined by running the program on a [Hetzner AX161](https://www.hetzner.com/dedicated-rootserver/ax161) dedicated server (32 core AMD EPYC™ 7502P (Zen2), 128 GB RAM).

Programs are run from  a RAM disk (i.o. the IO overhead for loading the file from disk is not relevant), using 8 cores of the machine.
Each contender must pass the 1BRC test suite (_/test.sh_).
The `hyperfine` program is used for measuring execution times of the launch scripts of all entries, i.e. end-to-end times are measured.
Each contender is run five times in a row.
The slowest and the fastest runs are discarded.
The mean value of the remaining three runs is the result for that contender and will be added to the results table above.
The exact same _measurements.txt_ file is used for evaluating all contenders.
See the script _evaluate.sh_ for the exact implementation of the evaluation steps.

## Prize

If you enter this challenge, you may learn something new, get to inspire others, and take pride in seeing your name listed in the scoreboard above.
Rumor has it that the winner may receive a unique 1️⃣🐝🏎️ t-shirt, too!

## FAQ

_Q: Can I use Kotlin or other JVM languages other than Java?_\
A: No, this challenge is focussed on Java only. Feel free to inofficially share implementations significantly outperforming any listed results, though.

_Q: Can I use non-JVM languages and/or tools?_\
A: No, this challenge is focussed on Java only. Feel free to inofficially share interesting implementations and results though. For instance it would be interesting to see how DuckDB fares with this task.

_Q: I've got an implementation—but it's not in Java. Can I share it somewhere?_\
A: Whilst non-Java solutions cannot be formally submitted to the challenge, you are welcome to share them over in the [Show and tell](https://github.com/gunnarmorling/1brc/discussions/categories/show-and-tell) GitHub discussion area.

_Q: Can I use JNI?_\
A: Submissions must be completely implemented in Java, i.e. you cannot write JNI glue code in C/C++. You could use AOT compilation of Java code via GraalVM though, either by AOT-compiling the entire application, or by creating a native library (see [here](https://www.graalvm.org/22.0/reference-manual/native-image/ImplementingNativeMethodsInJavaWithSVM/).

_Q: What is the encoding of the measurements.txt file?_\
A: The file is encoded with UTF-8.

_Q: Can I make assumptions on the names of the weather stations showing up in the data set?_\
A: No, while only a fixed set of station names is used by the data set generator, any solution should work with arbitrary UTF-8 station names
(for the sake of simplicity, names are guaranteed to contain no `;` or `\n` characters).

_Q: Can I copy code from other submissions?_\
A: Yes, you can. The primary focus of the challenge is about learning something new, rather than "winning". When you do so, please give credit to the relevant source submissions. Please don't re-submit other entries with no or only trivial improvements.

_Q: Which operating system is used for evaluation?_\
A: Fedora 39.

_Q: My solution runs in 2 sec on my machine. Am I the fastest 1BRC-er in the world?_\
A: Probably not :) 1BRC results are reported in wallclock time, thus results of different implementations are only comparable when obtained on the same machine. If for instance an implementation is faster on a 32 core workstation than on the 8 core evaluation instance, this doesn't allow for any conclusions. When sharing 1BRC results, you should also always share the result of running the baseline implementation on the same hardware.

_Q: Why_ 1️⃣🐝🏎️ _?_\
A: It's the abbreviation of the project name: **One** **B**illion **R**ow **C**hallenge.

## 1BRC on the Web

A list of external resources such as blog posts and videos, discussing 1BRC and specific implementations:

* [The One Billion Row Challenge Shows That Java Can Process a One Billion Rows File in Two Seconds ](https://www.infoq.com/news/2024/01/1brc-fast-java-processing), by Olimpiu Pop (interview)
* [Cliff Click discussing his 1BRC solution on the Coffee Compiler Club](https://www.youtube.com/watch?v=NJNIbgV6j-Y) (video)
* [1️⃣🐝🏎️🦆 (1BRC in SQL with DuckDB)](https://rmoff.net/2024/01/03/1%EF%B8%8F%E2%83%A3%EF%B8%8F-1brc-in-sql-with-duckdb/), by Robin Moffatt (blog post)
* [1 billion rows challenge in PostgreSQL and ClickHouse](https://ftisiot.net/posts/1brows/), by Francesco Tisiot (blog post)
* [The One Billion Row Challenge with Snowflake](https://medium.com/snowflake/the-one-billion-row-challenge-with-snowflake-f612ae76dbd5), by Sean Falconer (blog post)
* [One billion row challenge using base R](https://www.r-bloggers.com/2024/01/one-billion-row-challenge-using-base-r/), by  David Schoch (blog post)
* [1 Billion Row Challenge with Apache Pinot](https://hubertdulay.substack.com/p/1-billion-row-challenge-in-apache), by Hubert Dulay (blog post)
* [One Billion Row Challenge In C](https://www.dannyvankooten.com/blog/2024/1brc/), by Danny Van Kooten (blog post)
* [One Billion Row Challenge in Racket](https://defn.io/2024/01/10/one-billion-row-challenge-in-racket/), by Bogdan Popa (blog post)
* [The One Billion Row Challenge - .NET Edition](https://dev.to/mergeconflict/392-the-one-billion-row-challenge-net-edition), by Frank A. Krueger (podcast)
* [One Billion Row Challenge](https://curiouscoding.nl/posts/1brc/), by Ragnar Groot Koerkamp (blog post)
* [ClickHouse and The One Billion Row Challenge](https://clickhouse.com/blog/clickhouse-one-billion-row-challenge), by Dale McDiarmid (blog post)
* [One Billion Row Challenge & Azure Data Explorer](https://nielsberglund.com/post/2024-01-28-one-billion-row-challenge--azure-data-explorer/), by Niels Berglund (blog post)
* [One Billion Row Challenge - view from sidelines](https://www.chashnikov.dev/post/one-billion-row-challenge-view-from-sidelines), by Leo Chashnikov (blog post)
* [1 billion row challenge in SQL and Oracle Database](https://geraldonit.com/2024/01/31/1-billion-row-challenge-in-sql-and-oracle-database/), by Gerald Venzl (blog post)
* [One Billion Row Challenge: Learned So Far](https://gamlor.info/posts-output/2024-01-12-one-billion-row-challenge/en/), by Roman Stoffel (blog post)
* [One Billion Row Challenge in Racket](https://defn.io/2024/01/10/one-billion-row-challenge-in-racket/), by Bogdan Popa (blog post)
* [The 1 Billion row challenge with Singlestore](https://medium.com/@testily/the-1-billion-row-challenge-with-singlestore-224ce97e451f), by Anna Semjen (blog post)
* [1BRC in .NET among fastest on Linux: My Optimization Journey](https://hotforknowledge.com/2024/01/13/1brc-in-dotnet-among-fastest-on-linux-my-optimization-journey/), by Victor Baybekov (blog post)
* [One Billion Rows – Gerald’s Challenge](https://connor-mcdonald.com/2024/02/03/one-billion-rows-geralds-challenge/), by Connor McDonald (blog post)

## License

This code base is available under the Apache License, version 2.

## Code of Conduct

Be excellent to each other!
More than winning, the purpose of this challenge is to have fun and learn something new.
