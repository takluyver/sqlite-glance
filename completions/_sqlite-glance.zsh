#compdef _sqlite-glance.zsh sqlite-glance

function _sqlite-glance.zsh {
    local curcontext="$curcontext"
    local context state state_descr line
    typeset -A opt_args

    _arguments -C \
        "-h[Show help information]" \
        "--help[Show help information]" \
        "-V[Show version number]" \
        "--version[Show version number]" \
        "-n[Number of rows shown in table view]" \
        "--limit[Number of rows shown in table view]" \
        "--hidden[Show shadow tables, system tables & hidden columns]" \
        ":SQLite file:_files" \
        ":table/view name:->infile"

    case "$state" in
        infile)
                declare -a matches

                # List entries in the group, add the group path and a / suffix for
                # subgroups, and case-insensitively filter them against the text entered.
                matches=(
                    $(sqlite3 -init /dev/null -readonly "${line[1]}" "SELECT name FROM sqlite_master WHERE type='table' OR type='view'" \
                      | awk -v IGNORECASE=1 -v p="${line[2]}" \
                      'p==substr($0,0,length(p))')
                )

                # The -M match-spec argument allows case-insensitive matches
                compadd -M 'm:{[:lower:][:upper:]}={[:upper:][:lower:]}' -a matches
                ;;
    esac
}
