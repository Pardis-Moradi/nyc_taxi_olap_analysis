#!/bin/bash

ACTION=$1
OPTIMIZED_FLAG=$2

case "$ACTION" in
  setup)
    echo " Setting up project..."
    python3 scripts/init_clickhouse.py
    ;;
    
  reset)
    echo "🧼 Resetting project..."
    python3 scripts/reset_project.py
    ;;

  preprocess)
    echo "🚀 Starting preprocessing service..."
    python3 data_ingestion/run_service.py
    ;;

  run)
    echo "📊 Running query scenarios..."
    if [ "$FLAG" == "--optimized" ]; then
      python3 query_scenarios/scenario_runner.py --optimized

    elif [ "$FLAG" == "--server1" ]; then
      python3 clients_simulations1.py
      python3 server1.py

    elif [ "$FLAG" == "--server2" ]; then
      python3 clients_simulations2.py
      python3 server2.py

    else
      python3 query_scenarios/scenario_runner.py

    fi
    ;;

  *)
    echo "❌ Unknown command: $ACTION"
    echo "Usage:"
    echo "  ./run.sh reset"
    echo "  ./run.sh preprocess"
    echo "  ./run.sh run [--optimized]"
    echo "  ./run.sh run [--server1]"
    echo "  ./run.sh run [--server2]"
    ;;
esac

