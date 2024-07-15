#!/usr/bin/env bash
#
#% SYNOPSIS
#+    ${script_name} [-h] SOURCE_DIR DEST_DIR
#%
#% DESCRIPTION
#%    Scans SOURCE_DIR for UUID-named DwC-A archives
#%    and extracts meta.xml into DEST_DIR,
#%    where each have their own <uuid>.dir directory.
#%
#%    Useful for generating a manifest of publisher-declared DwC terms.
#%
#- IMPLEMENTATION
#-    author   valdeza
#-
# END_OF_HEADER

script_headsize=$(head -50 "$0" |grep -n "^# END_OF_HEADER" | cut -f1 -d:)
script_name="$(basename "$0")"

usage() { printf "usage: "; head "-${script_headsize:-99}" "$0" | grep -e "^#+" | sed -e "s/^#+[ ]*//g" -e "s/\${script_name}/${script_name}/g" ; }
usagefull() { head "-${script_headsize:-99}" "$0" | grep -e "^#[%+-]" | sed -e "s/^#[%+-]//g" -e "s/\${script_name}/${script_name}/g" ; }
scriptinfo() { head "-${script_headsize:-99}" "$0" | grep -e "^#-" | sed -e "s/^#-//g" -e "s/\${script_name}/${script_name}/g"; }

if [[ "$*" =~ ^-h$ ]] || [[ "$*" =~ ^--help$ ]]; then
  usagefull
  exit 0
fi

if [ $# -ne 2 ]; then 
  usage
  exit 1
fi

# usage: die exit_status message
die() {
  rc=$1
  shift
  printf 'fatal: %s\n' "$*" >&2
  exit "$rc"
}

srcdir="$(readlink --verbose --canonicalize-existing "$1")" || exit 1
dstdir="$(readlink --verbose --canonicalize-existing "$2")" || exit 1

find "$srcdir" -maxdepth 1 -regextype sed -regex '.*/[a-fA-F0-9\-]\{36\}' -print0 \
  | while read -r -d $'\0' f
do
  cd "$dstdir" || exit 1
  archive_uuid="$(basename "$f")"
  newdir="${archive_uuid}.dir"
  if ! mkdir -pv "$newdir" || ! cd "$newdir" ; then
    echo "err: skipping meta.xml extraction: $f" >&2
    continue
  fi
  unzip -uo "$f" 'meta.xml'
done
