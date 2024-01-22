# Environment
This file just contains some intel about the environment in use and what has been done to get it into that state.

## Machine Type

* Hetzner AX161, Dedicated Hosted Hardware
* CPU: AMD EPYC 7502P 32 cores / 64 threads @ 2.5 GHz
* Memory: 128 GB ECC DDR4 RAM
* 2x SAMSUNG MZQL2960HCJR-00A07, 1 TB, Software RAID-1
* CentOS 9, Linux 5.14.0-378.el9.x86_64

## Configuration

* SMT off
* Turbo Boost Off
* Filesystem EXT4 

## Details

### CPU
``` 
$ cat /proc/cpuinfo 
processor	: 0
vendor_id	: AuthenticAMD
cpu family	: 23
model		: 49
model name	: AMD EPYC 7502P 32-Core Processor
stepping	: 0
microcode	: 0x8301055
cpu MHz		: 2500.000
cache size	: 512 KB
physical id	: 0
siblings	: 32
core id		: 0
cpu cores	: 32
apicid		: 0
initial apicid	: 0
fpu		: yes
fpu_exception	: yes
cpuid level	: 16
wp		: yes
flags		: fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush mmx fxsr sse sse2 ht syscall nx mmxext fxsr_opt pdpe1gb rdtscp lm constant_tsc rep_good nopl nonstop_tsc cpuid extd_apicid aperfmperf rapl pni pclmulqdq monitor ssse3 fma cx16 sse4_1 sse4_2 movbe popcnt aes xsave avx f16c rdrand lahf_lm cmp_legacy svm extapic cr8_legacy abm sse4a misalignsse 3dnowprefetch osvw ibs skinit wdt tce topoext perfctr_core perfctr_nb bpext perfctr_llc mwaitx cpb cat_l3 cdp_l3 hw_pstate ssbd mba ibrs ibpb stibp vmmcall fsgsbase bmi1 avx2 smep bmi2 cqm rdt_a rdseed adx smap clflushopt clwb sha_ni xsaveopt xsavec xgetbv1 xsaves cqm_llc cqm_occup_llc cqm_mbm_total cqm_mbm_local clzero irperf xsaveerptr rdpru wbnoinvd arat npt lbrv svm_lock nrip_save tsc_scale vmcb_clean flushbyasid decodeassists pausefilter pfthreshold avic v_vmsave_vmload vgif v_spec_ctrl umip rdpid overflow_recov succor smca sme sev sev_es
bugs		: sysret_ss_attrs spectre_v1 spectre_v2 spec_store_bypass retbleed smt_rsb
bogomips	: 4990.70
TLB size	: 3072 4K pages
clflush size	: 64
cache_alignment	: 64
address sizes	: 43 bits physical, 48 bits virtual
power management: ts ttp tm hwpstate cpb eff_freq_ro [13] [14]
... more for all other cores
```

## Setup

### Turn SMT off
Disable during boot via boot-param, able to switch it on later again, if needed.

Add `nosmt` to grub boot config in `/etc/default/grub` 

```
# Added nosmt to command line
GRUB_CMDLINE_LINUX="biosdevname=0 crashkernel=auto rd.auto=1 consoleblank=0 nosmt"
```

Update boot config:
``` 
sudo grub2-mkconfig -o /boot/grub2/grub.cfg
```

### Turbo Off
Using the legacy `/etc/rc.local` concept to change things during boot:

```
# Turn SMT off via software as well, already got nosmt in grub
echo off >  /sys/devices/system/cpu/smt/control

# Turn off turbo boost
echo 0 |tee /sys/devices/system/cpu/cpufreq/boost
```
### Reduce Swapping
Reduce from default 60 to 10% memory pressure by adding `vm.swappiness = 10` to `/etc/sysctl.conf`.

## Verify
Check after boot if all settings have been applied. Can also be used to control these during runtime.

* SMT off: `cat /sys/devices/system/cpu/smt/active` must be 0
* SWAP: `cat /proc/sys/vm/swappiness` must be 10
* Turbo off: `cat /sys/devices/system/cpu/cpufreq/boost` must be 0




