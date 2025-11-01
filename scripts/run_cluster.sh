#!/usr/bin/env bash

# MapReduce Cluster Management Script

case "$1" in
    start)
        echo "Starting MapReduce cluster..."
        docker-compose up -d
        echo " ###Cluster started"
        echo ""
        echo "View logs:"
        echo "  docker-compose logs -f master"
        echo "  docker-compose logs -f worker1"
        ;;
    
    stop)
        echo "Stopping MapReduce cluster..."
        docker-compose down
        echo "### Cluster stopped"
        ;;
    
    restart)
        echo "Restarting MapReduce cluster..."
        docker-compose down
        docker-compose up -d
        echo "### Cluster restarted"
        ;;
    
    logs)
        if [ -z "$2" ]; then
            docker-compose logs -f
        else
            docker-compose logs -f "$2"
        fi
        ;;
    
    status)
        echo "Cluster Status:"
        docker-compose ps
        ;;
    
    kill-worker)
        if [ -z "$2" ]; then
            echo "Usage: ./run_cluster.sh kill-worker <worker_name>"
            echo "Example: ./run_cluster.sh kill-worker worker1"
        else
            echo "Killing $2 to test fault tolerance..."
            docker-compose kill "$2"
            echo "### $2 killed"
            echo "Watch master logs to see task reassignment:"
            echo "  docker-compose logs -f master"
        fi
        ;;
    
    rebuild)
        echo "Rebuilding containers..."
        docker-compose down
        docker-compose build
        docker-compose up -d
        echo "### Cluster rebuilt and started"
        ;;
    
    *)
        echo "MapReduce Cluster Management"
        echo ""
        echo "Usage: ./run_cluster.sh <command>"
        echo ""
        echo "Commands:"
        echo "  start         - Start the cluster"
        echo "  stop          - Stop the cluster"
        echo "  restart       - Restart the cluster"
        echo "  logs [name]   - View logs (optionally for specific service)"
        echo "  status        - Show cluster status"
        echo "  kill-worker   - Kill a worker to test fault tolerance"
        echo "  rebuild       - Rebuild containers and start"
        echo ""
        echo "Examples:"
        echo "  ./run_cluster.sh start"
        echo "  ./run_cluster.sh logs master"
        echo "  ./run_cluster.sh kill-worker worker2"
        ;;
esac