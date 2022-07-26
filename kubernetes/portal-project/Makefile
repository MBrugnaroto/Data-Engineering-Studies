kubectl := kubectl apply -f

all: init-configmaps init-db init-portal init-pvc init-system init-services

init-configmaps:
	${kubectl} configmaps/db-configmap.yaml
	${kubectl} configmaps/system-configmap.yaml

init-db:
	${kubectl} deployments/dp-news-db.yaml

init-portal:
	${kubectl} deployments/dp-news-portal.yaml

init-pvc:
	${kubectl} statefulsets/pvc/pvc-image.yaml
	${kubectl} statefulsets/pvc/pvc-session.yaml

init-system: 
	${kubectl} statefulsets/ss-news-system.yaml

init-services:
	${kubectl} services/svc-news-db.yaml
	${kubectl} services/svc-news-portal.yaml
	${kubectl} services/svc-news-system.yaml
