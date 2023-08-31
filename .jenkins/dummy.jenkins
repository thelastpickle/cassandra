pipeline {
    agent {
        kubernetes {
            yaml '''
apiVersion: v1
kind: Pod
metadata:
  name: andreas-test
spec:
  containers:
  - name: jnlp
    image: jenkins/inbound-agent:4.3-4-jdk11
    command: ["sleep", "10000"]
'''

        }
    }

    stages {
        stage('Say Hello World') {
            steps {
                container("jnlp") {
                    echo "Hello World!"
                }
            }
        }
    }
}
