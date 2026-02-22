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
                output=$(sqlite-glance --completion "${line[1]}" "${line[2]}")
                matches=( ${(0)output} )  # Split on null bytes

                # The -M match-spec argument allows case-insensitive matches
                compadd -M 'm:{[:lower:][:upper:]}={[:upper:][:lower:]}' -a matches
                ;;
    esac
}
