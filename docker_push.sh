#!/bin/bash
git add .
git commit -m "debug"
git push origin dev
docker build  --no-cache -t neurodatalab/es-parser .
docker push neurodatalab/es-parser
