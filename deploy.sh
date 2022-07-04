#!/bin/bash
# A simple variable example
login="retang"
remoteFolder="/tmp/$login/"
splitsFolder="/tmp/$login/splits"
ServerFileName="SimpleServerProgram"
SplitFileName="Split"
SplitCountFileName="SplitCount"
MachineListFileName="MachineList"
ListenerReducerFileName="ListenerReducer"
WorkerSenderFileName="WorkerSender"
WordFileName="Word"
ValueThenKeyComparator="ValueThenKeyComparator"
fileExtension=".java"
options="-cp '$SplitFileName$fileExtension;$SplitCountFileName$fileExtension'"
# computers=("tp-t310-13.enst.fr")
# computers=("tp-t310-13.enst.fr" "tp-t310-14.enst.fr")
# computers=("tp-t310-13.enst.fr" "tp-t310-14.enst.fr" "tp-t310-16.enst.fr")
# computers=("tp-t310-13.enst.fr" "tp-t310-14.enst.fr" "tp-t310-16.enst.fr" "tp-1a207-11.enst.fr")
computers=("tp-t310-13.enst.fr" "tp-t310-14.enst.fr" "tp-t310-16.enst.fr" "tp-1a207-11.enst.fr" "tp-3b01-02.enst.fr")
for c in ${computers[@]}; do
  command0=("ssh" "$login@$c" "lsof -ti | xargs kill -9")
  command1=("ssh" "$login@$c" "rm -rf $remoteFolder;mkdir $remoteFolder;cd $remoteFolder;mkdir $splitsFolder")
  command2=("scp" "$ServerFileName$fileExtension" "$MachineListFileName$fileExtension" "$SplitFileName$fileExtension" "$SplitCountFileName$fileExtension" "$ListenerReducerFileName$fileExtension" "$WorkerSenderFileName$fileExtension" "$WordFileName$fileExtension" "$ValueThenKeyComparator$fileExtension" "$login@$c:$remoteFolder")
  command3=("ssh" "$login@$c" "cd $remoteFolder;javac $ServerFileName$fileExtension;java $ServerFileName")
  echo ${command0[*]}
  "${command0[@]}"
  echo ${command1[*]}
  "${command1[@]}"
  echo ${command2[*]}
  "${command2[@]}"
  echo ${command3[*]}
  "${command3[@]}" &
done