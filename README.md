# 1️⃣🐝🏎️ The One Billion Row Challenge

_Status Jan 12: As there has been such a large number of entries to this challenge so far (100+), and this is becoming hard to manage, please only create new submissions if you expect them to run in 10 seconds or less on the evaluation machine._

_Status Jan 1: This challenge is [open for submissions](https://www.morling.dev/blog/one-billion-row-challenge/)!_

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

| # | Result (m:s.ms) | Implementation     | JDK | Submitter     | Notes     |
|---|-----------------|--------------------|-----|---------------|-----------|
| 1* | 00:02.461 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_artsiomkorzun.java)| 21.0.1-graal | [Artsiom Korzun](https://github.com/artsiomkorzun) | GraalVM native binary |
| 1* | 00:02.477 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_abeobk.java)| 21.0.1-graal | [Van Phu DO](https://github.com/abeobk) | GraalVM native binary |
| 3* | 00:02.552 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_thomaswue.java)| 21.0.1-graal | [Thomas Wuerthinger](https://github.com/thomaswue), [Quan Anh Mai](https://github.com/merykitty), [Alfonso² Peterssen](https://github.com/mukel) | GraalVM native binary |
| 3*  | 00:02.571 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_royvanrijn.java)| 21.0.1-graal | [Roy van Rijn](https://github.com/royvanrijn) | GraalVM native binary |
| 3* | 00:02.575 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_merykittyunsafe.java)| 21.0.1-open | [Quan Anh Mai](https://github.com/merykitty) | Quan Anh Mai's implementation, using `Unsafe` |
|   | 00:02.909 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_jerrinot.java)| 21.0.1-graal | [Jaromir Hamala](https://github.com/jerrinot) |  |
|   | 00:03.258 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_merykitty.java)| 21.0.1-open | [Quan Anh Mai](https://github.com/merykitty) |  |
|   | 00:03.376 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_mtopolnik.java)| 21.0.1-graal | [Marko Topolnik](https://github.com/mtopolnik) |  |
|   | 00:03.714 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_hundredwatt.java)| 21.0.1-graal | [Jason Nochlin](https://github.com/hundredwatt) |  |
|   | 00:03.718 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_zerninv.java)| 21.0.1-graal | [zerninv](https://github.com/zerninv) |  |
|   | 00:03.902 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_jparera.java)| 21.0.1-open | [Juan Parera](https://github.com/jparera) |  |
|   | 00:03.959 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_gonix.java)| 21.0.1-open | [gonix](https://github.com/gonix) |  |
|   | 00:03.966 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_jincongho.java)| 21.0.1-open | [Jin Cong Ho](https://github.com/jincongho) |  |
|   | 00:04.066 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_JesseVanRooy.java)| 21.0.1-open | [JesseVanRooy](https://github.com/JesseVanRooy) |  |
|   | 00:04.154 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_iziamos.java)| 21.0.1-open | [John Ziamos](https://github.com/iziamos) |  |
|   | 00:04.365 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_ebarlas.java)| 21.0.1-graal | [Elliot Barlas](https://github.com/ebarlas) |  |
|   | 00:04.714 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_roman-r-m.java)| 21.0.1-graal | [Roman Musin](https://github.com/roman-r-m) |  |
|   | 00:04.741 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_cliffclick.java)| 21.0.1-open | [Cliff Click](https://github.com/cliffclick) |  |
|   | 00:04.823 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_JamalMulla.java)| 21.0.1-graal | [Jamal Mulla](https://github.com/JamalMulla) |  |
|   | 00:04.920 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_vemana.java)| 21.0.1-graal | [Subrahmanyam](https://github.com/vemana) |  |
|   | 00:04.959 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_yavuztas.java)| 21.0.1-graal | [Yavuz Tas](https://github.com/yavuztas) |  |
|   | 00:05.142 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_arjenw.java)| 21.0.1-open | [Arjen Wisse](https://github.com/arjenw) |  |
|   | 00:05.235 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_unbounded.java)| 21.0.1-open | [unbounded](https://github.com/unbounded) |  |
|   | 00:05.336 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_plevart.java)| 21.0.1-tem | [Peter Levart](https://github.com/plevart) |  |
|   | 00:05.478 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_obourgain.java)| 21.0.1-open | [Olivier Bourgain](https://github.com/obourgain) |  |
|   | 00:05.887 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_charlibot.java)| 21.0.1-graal | [Charlie Evans](https://github.com/charlibot) |  |
|   | 00:05.960 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_vaidhy.java)| 21.0.1-graal | [Vaidhy Mayilrangam](https://github.com/vaidhy) |  |
|   | 00:05.979 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_spullara.java)| 21.0.1-graal | [Sam Pullara](https://github.com/spullara) |  |
|   | 00:06.166 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_isolgpus.java)| 21.0.1-open | [Jamie Stansfield](https://github.com/isolgpus) |  |
|   | 00:06.257 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_flippingbits.java)| 21.0.1-graal | [Stefan Sprenger](https://github.com/flippingbits) |  |
|   | 00:06.415 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_armandino.java)| 21.0.1-open | [Arman Sharif](https://github.com/armandino) |  |
|   | 00:06.654 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_jbachorik.java)| 21.0.1-graal | [Jaroslav Bachorik](https://github.com/jbachorik) |  |
|   | 00:06.576 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_as-com.java)| 21.0.1-open | [Andrew Sun](https://github.com/as-com) |  |
|   | 00:06.715 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_algirdasrascius.java)| 21.0.1-open | [Algirdas Raščius](https://github.com/algirdasrascius) |  |
|   | 00:06.872 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_ianopolousfast.java)| 21.0.1-open | [Dr Ian Preston](https://github.com/ianopolousfast) |  |
|   | 00:07.240 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_giovannicuccu.java)| java | [giovannicuccu](https://github.com/giovannicuccu) |  |
|   | 00:07.680 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_C5H12O5.java)| 21.0.1-graal | [Xylitol](https://github.com/C5H12O5) |  |
|   | 00:07.730 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_jotschi.java)| 21.0.1-open | [Johannes Schüth](https://github.com/jotschi) |  |
|   | 00:07.925 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_ricardopieper.java)| 21.0.1-graal | [Ricardo Pieper](https://github.com/ricardopieper) |  |
|   | 00:07.913 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_parkertimmins.java)| 21.0.1-open | [parkertimmins](https://github.com/parkertimmins) |  |
|   | 00:08.167 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_ddimtirov.java)| 21.0.1-tem | [Dimitar Dimitrov](https://github.com/ddimtirov) |  |
|   | 00:08.214 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_deemkeen.java)| 21.0.1-open | [deemkeen](https://github.com/deemkeen) |  |
|   | 00:08.398 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_artpar.java)| 21.0.1-open | [Parth Mudgal](https://github.com/artpar) |  |
|   | 00:08.489 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_gnabyl.java)| 21.0.1-graal | [Bang NGUYEN](https://github.com/gnabyl) |  |
|   | 00:08.517 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_ags313.java)| 21.0.1-graal | [ags](https://github.com/ags313) |  |
|   | 00:08.622 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_kuduwa-keshavram.java)| 21.0.1-graal | [Keshavram Kuduwa](https://github.com/kuduwa-keshavram) |  |
|   | 00:08.689 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_gamlerhart.java)| 21.0.1-open | [Roman Stoffel](https://github.com/gamlerhart) |  |
|   | 00:08.752 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_anitasv.java)| 21.0.1-graal | [Anita SV](https://github.com/anitasv) |  |
|   | 00:08.892 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_fatroom.java)| 21.0.1-open | [Roman Romanchuk](https://github.com/fatroom) |  |
|   | 00:09.020 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_yemreinci.java)| 21.0.1-open | [yemreinci](https://github.com/yemreinci) |  |
|   | 00:09.071 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_gabrielreid.java)| 21.0.1-open | [Gabriel Reid](https://github.com/gabrielreid) |  |
|   | 00:09.352 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_filiphr.java)| 21.0.1-graal | [Filip Hrisafov](https://github.com/filiphr) |  |
|   | 00:09.867 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_ricardopieper.java)| 21.0.1-graal | [Ricardo Pieper](https://github.com/ricardopieper) |  |
|   | 00:09.945 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_japplis.java)| 21.0.1-open | [Anthony Goubard](https://github.com/japplis) |  |
|   | 00:10.092 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_phd3.java)| 21.0.1-graal | [Pratham](https://github.com/phd3) |  |
|   | 00:10.127 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_artpar.java)| 21.0.1-open | [Parth Mudgal](https://github.com/artpar) |  |
|   | 00:11.577 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_netrunnereve.java)| 21.0.1-open | [Eve](https://github.com/netrunnereve) |  |
|   | 00:10.473 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_raipc.java)| 21.0.1-open | [Anton Rybochkin](https://github.com/raipc) |  |
|   | 00:11.119 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_lawrey.java)| 21.0.1-open | [lawrey](https://github.com/lawrey) |  |
|   | 00:11.167 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_palmr.java)| 21.0.1-open | [Nick Palmer](https://github.com/palmr) |  |
|   | 00:11.230 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_adriacabeza.java)| 21.0.1-graal | [adri](https://github.com/adriacabeza) |  |
|   | 00:11.405 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_imrafaelmerino.java)| 21.0.1-graal | [Rafael Merino García](https://github.com/imrafaelmerino) |  |
|   | 00:11.433 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_jatingala.java)| 21.0.1-graal | [Jatin Gala](https://github.com/jatingala) |  |
|   | 00:11.805 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_coolmineman.java)| 21.0.1-graal | [Cool_Mineman](https://github.com/coolmineman) |  |
|   | 00:11.878 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_karthikeyan97.java)| 21.0.1-open | [karthikeyan97](https://github.com/karthikeyan97) |  |
|   | 00:11.934 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_arjenvaneerde.java)| 21.0.1-open | [arjenvaneerde](https://github.com/arjenvaneerde) |  |
|   | 00:12.051 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_dmitry-midokura.java)| 21.0.1-open | [Dmitry Bufistov](https://github.com/dmitry-midokura) |  |
|   | 00:12.102 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_YannMoisan.java)| java | [Yann Moisan](https://github.com/YannMoisan) |  |
|   | 00:12.220 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_richardstartin.java)| 21.0.1-open | [Richard Startin](https://github.com/richardstartin) |  |
|   | 00:12.495 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_SamuelYvon.java)| 21.0.1-graal | [Samuel Yvon](https://github.com/SamuelYvon) | GraalVM native binary |
|   | 00:12.568 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_MeanderingProgrammer.java)| 21.0.1-graal | [Vlad](https://github.com/MeanderingProgrammer) |  |
|   | 00:12.800 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_yonatang.java)| java | [Yonatan Graber](https://github.com/yonatang) |  |
|   | 00:13.013 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_thanhtrinity.java)| 21.0.1-graal | [Thanh Duong](https://github.com/thanhtrinity) |  |
|   | 00:13.071 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_ianopolous.java)| 21.0.1-open | [Dr Ian Preston](https://github.com/ianopolous) |  |
|   | 00:13.817 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_entangled90.java)| 21.0.1-open | [Carlo](https://github.com/entangled90) |  |
|   | 00:14.502 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_eriklumme.java)| 21.0.1-graal | [eriklumme](https://github.com/eriklumme) |  |
|   | 00:14.772 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_kevinmcmurtrie.java)| 21.0.1-open | [Kevin McMurtrie](https://github.com/kevinmcmurtrie) |  |
|   | 00:14.867 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_berry120.java)| 21.0.1-open | [Michael Berry](https://github.com/berry120) |  |
|   | 00:15.662 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_semotpan.java)| 21.0.1-open | [Serghei Motpan](https://github.com/semotpan) |  |
|   | 00:17.179 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_jgrateron.java)| 21.0.1-open | [Jairo Graterón](https://github.com/jgrateron) |  |
|   | 00:17.490 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_kgeri.java)| 21.0.1-open | [Gergely Kiss](https://github.com/kgeri) |  |
|   | 00:17.255 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_tkosachev.java)| 21.0.1-open | [tkosachev](https://github.com/tkosachev) |  |
|   | 00:17.520 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_faridtmammadov.java)| 21.0.1-open | [Farid](https://github.com/faridtmammadov) |  |
|   | 00:17.717 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_omarchenko4j.java)| 21.0.1-open | [Oleh Marchenko](https://github.com/omarchenko4j) |  |
|   | 00:17.815 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_hallvard.java)| 21.0.1-open | [Hallvard Trætteberg](https://github.com/hallvard) |  |
|   | 00:17.932 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_plbpietrz.java)| 21.0.1-open | [Bartłomiej Pietrzyk](https://github.com/plbpietrz) |  |
|   | 00:18.251 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_seijikun.java)| 21.0.1-graal | [Markus Ebner](https://github.com/seijikun) |  |
|   | 00:18.448 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_moysesb.java)| 21.0.1-open | [Moysés Borges Furtado](https://github.com/moysesb) |  |
|   | 00:18.771 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_davecom.java)| 21.0.1-graal | [David Kopec](https://github.com/davecom) |  |
|   | 00:18.902 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_maximz101.java)| 21.0.1-graal | [Maxime](https://github.com/maximz101) |  |
|   | 00:19.357 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_truelive.java)| 21.0.1-graalce | [Roman Schweitzer](https://github.com/truelive) |  |
|   | 00:20.691 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_Kidlike.java)| 21.0.1-graal | [Kidlike](https://github.com/Kidlike) | GraalVM native binary |
|   | 00:21.989 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_couragelee.java)| 21.0.1-open | [couragelee](https://github.com/couragelee) |  |
|   | 00:22.457 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_rby.java)| 21.0.1-open | [Ramzi Ben Yahya](https://github.com/rby) |  |
|   | 00:22.471 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_0xshivamagarwal.java)| 21.0.1-open | [Shivam Agarwal](https://github.com/0xshivamagarwal) |  |
|   | 00:26.500 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_felix19350.java)| 21.0.1-open | [Bruno Félix](https://github.com/felix19350) |  |
|   | 00:28.381 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_bjhara.java)| 21.0.1-open | [Hampus](https://github.com/bjhara) |  |
|   | 00:29.741 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_xpmatteo.java)| 21.0.1-open | [Matteo Vaccari](https://github.com/xpmatteo) |  |
|   | 00:32.018 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_padreati.java)| 21.0.1-open | [Aurelian Tutuianu](https://github.com/padreati) |  |
|   | 00:34.388 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_twobiers.java)| 21.0.1-tem | [Tobi](https://github.com/twobiers) |  |
|   | 00:35.875 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_MahmoudFawzyKhalil.java)| 21.0.1-open | [MahmoudFawzyKhalil](https://github.com/MahmoudFawzyKhalil) |  |
|   | 00:36.180 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_hchiorean.java)| 21.0.1-open | [Horia Chiorean](https://github.com/hchiorean) |  |
|   | 00:36.991 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_kumarsaurav123.java)| 21.0.1-open | [kumarsaurav123](https://github.com/kumarsaurav123) |  |
|   | 00:38.340 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_AbstractKamen.java)| 21.0.1-open | [AbstractKamen](https://github.com/AbstractKamen) |  |
|   | 00:41.982 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_criccomini.java)| 21.0.1-open | [Chris Riccomini](https://github.com/criccomini) |  |
|   | 00:42.893 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_javamak.java)| 21.0.1-open | [javamak](https://github.com/javamak) |  |
|   | 00:46.597 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_maeda6uiui.java)| 21.0.1-open | [Maeda-san](https://github.com/maeda6uiui) |  |
|   | 00:58.811 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_Ujjwalbharti.java)| 21.0.1-open | [Ujjwal Bharti](https://github.com/Ujjwalbharti) |  |
|   | 01:05.094 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_mudit-saxena.java)| 21.0.1-open | [Mudit Saxena](https://github.com/mudit-saxena) |  |
|   | 01:06.790 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_khmarbaise.java)| 21.0.1-open | [Karl Heinz Marbaise](https://github.com/khmarbaise) |  |
|   | 01:06.944 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_santanu.java)| 21.0.1-open | [santanu](https://github.com/santanu) |  |
|   | 01:07.014 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_pedestrianlove.java)| 21.0.1-open | [pedestrianlove](https://github.com/pedestrianlove) |  |
|   | 01:08.811 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_alesj.java)| 21.0.1-open | [Aleš Justin](https://github.com/alesj) |  |
|   | 01:08.908 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_itaske.java)| 21.0.1-open | [itaske](https://github.com/itaske) |  |
|   | 01:09.595 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_agoncal.java)| 21.0.1-tem | [Antonio Goncalves](https://github.com/agoncal) |  |
|   | 01:09.882 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_rprabhu.java)| 21.0.1-open | [Prabhu R](https://github.com/rprabhu) |  |
|   | 01:14.815 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_anandmattikopp.java)| 21.0.1-open | [twohardthings](https://github.com/anandmattikopp) |  |
|   | 01:25.801 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_ivanklaric.java)| 21.0.1-open | [ivanklaric](https://github.com/ivanklaric) |  |
|   | 01:33.594 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_gnmathur.java)| 21.0.1-open | [Gaurav Mathur](https://github.com/gnmathur) |  |
|   | 01:56.607 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_abfrmblr.java)| 21.0.1-open | [Abhilash](https://github.com/abfrmblr) |  |
|   | 03:43.521 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_yehwankim23.java)| 21.0.1-open | [김예환 Ye-Hwan Kim (Sam)](https://github.com/yehwankim23) |  |
|   | 03:59.760 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_fragmede.java)| 21.0.1-open | [Samson](https://github.com/fragmede) |  |
|   | ---       | | | | |
|   | 04:49.679 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_baseline.java) (Baseline) | 21.0.1-open | [Gunnar Morling](https://github.com/gunnarmorling) |  |

\* These two entries have such a similar runtime (below the error margin I can reliably measure), that they share position #1 in the leaderboar.

Note that I am not super-scientific in the way I'm running the contenders
(see [Evaluating Results](#evaluating-results) for the details).
This is not a high-fidelity micro-benchmark and there can be variations of ~ +-5% between runs.
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
Here are the results from running the top 15 entries (as of commit [2c26b511](https://github.com/gunnarmorling/1brc/commit/2c26b511e741f4d96a51dda831001946ea27a591)) on all 32 cores / 64 threads (i.e. SMT is enabled) of the machine:

| # | Result (m:s.ms) | Implementation     | JDK | Submitter     | Notes     |
|---|-----------------|--------------------|-----|---------------|-----------|
| 1 | 00:00.799 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_thomaswue.java)| 21.0.1-graal | [Thomas Wuerthinger](https://github.com/thomaswue) | GraalVM native binary |
| 2 | 00:00.933 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_royvanrijn.java)| 21.0.1-graal | [Roy van Rijn](https://github.com/royvanrijn) | GraalVM native binary |
| 3 | 00:01.236 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_artsiomkorzun.java)| 21.0.1-graal | [Artsiom Korzun](https://github.com/artsiomkorzun) |  |
|   | 00:01.380 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_merykittyunsafe.java)| 21.0.1-open | [merykittyunsafe](https://github.com/merykittyunsafe) |  |
|   | 00:01.383 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_cliffclick.java)| 21.0.1-open | [Cliff Click](https://github.com/cliffclick) |  |
|   | 00:01.429 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_iziamos.java)| 21.0.1-open | [John Ziamos](https://github.com/iziamos) |  |
|   | 00:01.464 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_obourgain.java)| 21.0.1-open | [Olivier Bourgain](https://github.com/obourgain) |  |
|   | 00:01.603 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_abeobk.java)| 21.0.1-open | [Van Phu DO](https://github.com/abeobk) |  |
|   | 00:01.748 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_yavuztas.java)| 21.0.1-graal | [Yavuz Tas](https://github.com/yavuztas) |  |
|   | 00:01.778 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_merykitty.java)| 21.0.1-open | [Quan Anh Mai](https://github.com/merykitty) |  |
|   | 00:01.942 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_mtopolnik.java)| 21.0.1-graal | [Marko Topolnik](https://github.com/mtopolnik) |  |
|   | 00:01.972 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_ebarlas.java)| 21.0.1-graal | [Elliot Barlas](https://github.com/ebarlas) |  |
|   | 00:02.111 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_JamalMulla.java)| 21.0.1-graal | [Jamal Mulla](https://github.com/JamalMulla) |  |
|   | 00:02.644 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_vaidhy.java)| 21.0.1-graal | [Vaidhy Mayilrangam](https://github.com/vaidhy) |  |
|   | 00:03.697 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_hundredwatt.java)| 21.0.1-graal | [Jason Nochlin](https://github.com/hundredwatt) |  |

#### 10K Key Set

The 1BRC challenge data set contains 413 distinct weather stations, whereas the rules allow for 10,000 different station names to occur.
Here are the results from running the top 15 entries (as of commit [2c26b511](https://github.com/gunnarmorling/1brc/commit/2c26b511e741f4d96a51dda831001946ea27a591)) against 1,000,000,000 measurement values across 10K stations (created via _./create_measurements3.sh 1000000000_),
using eight cores on the evaluation machine:

| # | Result (m:s.ms) | Implementation     | JDK | Submitter     | Notes     |
|---|-----------------|--------------------|-----|---------------|-----------|
| 1 | 00:04.589 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_artsiomkorzun.java)| 21.0.1-graal | [Artsiom Korzun](https://github.com/artsiomkorzun) |  |
| 2 | 00:05.296 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_royvanrijn.java)| 21.0.1-graal | [Roy van Rijn](https://github.com/royvanrijn) | GraalVM native binary |
| 3 | 00:05.308 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_thomaswue.java)| 21.0.1-graal | [Thomas Wuerthinger](https://github.com/thomaswue) | GraalVM native binary |
|   | 00:05.881 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_mtopolnik.java)| 21.0.1-graal | [Marko Topolnik](https://github.com/mtopolnik) |  |
|   | 00:07.120 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_JamalMulla.java)| 21.0.1-graal | [Jamal Mulla](https://github.com/JamalMulla) |  |
|   | 00:07.915 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_cliffclick.java)| 21.0.1-open | [Cliff Click](https://github.com/cliffclick) |  |
|   | 00:08.979 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_yavuztas.java)| 21.0.1-graal | [Yavuz Tas](https://github.com/yavuztas) |  |
|   | 00:10.052 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_merykittyunsafe.java)| 21.0.1-open | [merykittyunsafe](https://github.com/merykittyunsafe) |  |
|   | 00:10.134 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_vaidhy.java)| 21.0.1-graal | [Vaidhy Mayilrangam](https://github.com/vaidhy) |  |
|   | 00:10.599 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_ebarlas.java)| 21.0.1-graal | [Elliot Barlas](https://github.com/ebarlas) |  |
|   | 00:12.750 | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_merykitty.java)| 21.0.1-open | [Quan Anh Mai](https://github.com/merykitty) |  |
|   | ---       | | | | |
|   | DNF | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_hundredwatt.java)| 21.0.1-graal | [Jason Nochlin](https://github.com/hundredwatt) | Didn't complete in 60 sec |
|   | DNF | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_abeobk.java)| 21.0.1-open | [Van Phu DO](https://github.com/abeobk) | Didn't complete in 60 sec |
|   | DNF | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_iziamos.java)| 21.0.1-open | [John Ziamos](https://github.com/iziamos) | Didn't complete in 60 sec |
|   | DNF | [link](https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CalculateAverage_obourgain.java)| 21.0.1-open | [Olivier Bourgain](https://github.com/obourgain) | Failed with java.lang.OutOfMemoryError: Java heap space |

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
  * Create a copy of _CalculateAverage.java_, named _CalculateAverage\_<your_GH_user>.java_, e.g. _CalculateAverage\_doloreswilson.java_.
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

## Sponsorship

A big thank you to my employer [Decodable](https://www.decodable.co/) for funding the evaluation environment and supporting this challenge!
 
## License

This code base is available under the Apache License, version 2.

## Code of Conduct

Be excellent to each other!
More than winning, the purpose of this challenge is to have fun and learn something new.
