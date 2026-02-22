# Completions for bash; copy to ~/.local/share/bash-completions/completions/
# Requires bash-completion, which is packaged by various Linux distros.

_sqlite-glance()
{
    local cur prev opts 
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"

    # Complete options
    if [[ ${cur} = -* ]]; then
      opts="-h --help -V --version -w --where -n --limit --hidden"
      compgen -V COMPREPLY -W "${opts}" -- "${cur}"
      return 0
    fi

    # Complete paths inside file
    if [[ -f ${prev} ]]; then
      # List tables/views matching the prefix
      mapfile -d '' COMPREPLY < <(sqlite-glance --completion "${prev}" "${cur}")
      return 0
    fi
}

complete -o default -o nospace -F _sqlite-glance sqlite-glance
