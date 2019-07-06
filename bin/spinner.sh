#!/bin/bash

spin()
{
  spinner="/|\\-/|\\-"
  while :
  do
    for i in `seq 0 7`
    do
      echo -n "${spinner:$i:1}"
      # Backspace
      echo -en "\010"
      sleep 0.5
    done
  done
}

start_spin()
{
  # Start the Spinner:
  spin &
  # Make a note of its Process ID (PID):
  SPIN_PID=$!
  # Kill the spinner on any signal, including our own exit.
  trap "kill -9 $SPIN_PID" `seq 0 15`
}