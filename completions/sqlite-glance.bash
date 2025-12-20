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
      # List tables, case-insensitively filter them against the text entered.
      mapfile -t COMPREPLY < <(sqlite3 -init /dev/null -safe -readonly "${prev}" "SELECT name FROM pragma_table_list() WHERE schema='main'" \
          | awk -v IGNORECASE=1 -v p="${cur}" \
              'p==substr($0,0,length(p))' \
     )
      return 0
    fi
}

complete -o default -o nospace -F _sqlite-glance sqlite-glance
