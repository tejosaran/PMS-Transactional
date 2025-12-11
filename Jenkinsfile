pipeline {
    agent any

    environment {
        DOCKERHUB_REPO = "tejosaran123/pms-transactional"
        IMAGE_TAG = "latest"
        EC2_HOST = "ubuntu@3.144.102.78"
    }

    stages {
        

       

        stage('Git Checkout') {
    steps {
        checkout([$class: 'GitSCM',
            branches: [[name: 'main']],
            extensions: [[$class: 'RelativeTargetDirectory', relativeTargetDir: '.']],
            userRemoteConfigs: [[url: 'https://github.com/pms-org/pms-transactional.git']]
        ])
    }
}


        stage('Debug Workspace') {
            steps {
                sh 'pwd'
                sh 'ls -R .'
            }
        }

       stage('Build Docker Image') {
    steps {
        sh """
           docker build --progress=plain --no-cache -t ${DOCKERHUB_REPO}:${IMAGE_TAG} .

        """
    }
}




        stage('Login & Push to DockerHub') {
            steps {
                withCredentials([usernamePassword(
                    credentialsId: 'dockerhub-creds',
                    usernameVariable: 'DOCKER_USER',
                    passwordVariable: 'DOCKER_PASS'
                )]) {
                    sh """
                    echo "$DOCKER_PASS" | docker login -u "$DOCKER_USER" --password-stdin
                    docker push ${DOCKERHUB_REPO}:${IMAGE_TAG}
                    """
                }
            }
        }

        stage('Deploy to EC2') {
            steps {
                sshagent(['ec2-ssh-key']) {
                    withCredentials([file(credentialsId: 'pms-env-file', variable: 'ENV_FILE')]) {

                        // Copy compose file
                        sh """
                        scp -o StrictHostKeyChecking=no \
                            docker/prod-docker-compose.yml \
                            ${EC2_HOST}:/home/ubuntu/docker-compose.yml
                        """

                        // Copy .env inside EC2 from Jenkins secret file
                        sh """
                        scp -o StrictHostKeyChecking=no ${ENV_FILE} ${EC2_HOST}:/home/ubuntu/.env
                        """

                        // Deploy containers
                        sh """
                        ssh -o StrictHostKeyChecking=no ${EC2_HOST} '
                            docker pull ${DOCKERHUB_REPO}:${IMAGE_TAG} &&
                            docker compose down &&
                            docker compose up -d
                        '
                        """
                    }
                }
            }
        }
    }

    post {
        success { echo "Deployment Successful" }
        failure { echo "Deployment Failed" }
    }

}