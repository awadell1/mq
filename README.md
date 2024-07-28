# MQ - a pretty job status cli

`mq` can be installed using [`pipx`](https://pipx.pypa.io/stable/) with:
```shell
pipx install git+https://github.com/awadel1/mq.git
```

## Commands

First and foremost, `mq` is a pretty job status cli for HPC job queues
```shell
awadell at lh-login2 in …/mq on  main [!] is 📦 v0.1.0 via 🐍 v3.11.5 (mq-py3.11) took 5s
❯ mq

 id         name           st       N    queue           host
 ─────────────────────────────────────────────────────────────────────────
 21802155   sbatch      COMPLETED   -    venkvis-debug   lh1702
 21803075   sbatch      COMPLETED   -    venkvis-debug   lh1702
 21806211   sbatch       FAILED     -    venkvis-debug   lh1702
 21812961   sbatch      COMPLETED   -    venkvis-debug   lh1702
 21814677   sbatch      COMPLETED   -    venkvis-debug   lh1702
 21816475   submit.sh    FAILED     -    venkvis-debug   lh1702
 21817279   sbatch       RUNNING    -    venkvis-debug   lh1702
```


## Monitoring Job Output
`mq cat` and `mq tail` can be used to display a job's standard output. By default, they print output of the last submitted job, or a specific job (i.e. `mq cat 21820125`).

Use `mq tail --watch` to get something akin to `watch tail job.out`, but without hassle of specifying the actual path.

