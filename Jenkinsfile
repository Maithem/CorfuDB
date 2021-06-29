pipeline {
  agent any
  stages {
    stage('mvn build') {
      steps {
        build 'mvn clean install -DskipTests -T4'
      }
    }

  }
}