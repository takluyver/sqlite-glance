## Shell completions for sqlite-glance

The scripts here add completions for table & view names inside a database, as well as command-line options.

### Bash

Copy `sqlite-glance.bash` into `~/.local/share/bash-completion/completions`, and start a new shell.
Or `$XDG_DATA_HOME/bash-completion/completions` if you've set `XDG_DATA_HOME`.

This requires [bash-completion](https://github.com/scop/bash-completion/),
which is packaged in major Linux distros and often seems to be installed
by default.

### zsh

Copy `sqlite-glance.zsh` into `~/.local/share/zsh-completions/` (or a location of your choice).
Ensure that `~/.zshrc` contains something like this:

```shell
fpath=("~/.local/share/zsh-completions" $fpath)
compinit
```

### Other shells

PRs welcome.
