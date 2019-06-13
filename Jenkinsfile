pipeline {
  agent any
  stages {
    stage('Checkout') {
      steps {
        git(url: 'https://github.com/Kent1/PCAP-to-Athena.git', branch: 'master')
      }
    }
    stage('Maxmind') {
      parallel {
        stage('Maxmind') {
          steps {
            sh './maxmind/download_maxmind_geo_ip_db.sh'
          }
        }
        stage('Libraries SIDN & Athena driver') {
          steps {
            sh './lib/download_libs.sh'
          }
        }
      }
    }
    stage('Build') {
      steps {
        sh './mvnw install -DskipTests=true -Dmaven.javadoc.skip=true -B -V'
      }
    }
    stage('Unit tests') {
      steps {
        sh './mvnw test -B'
      }
    }
    stage('Integration tests') {
      steps {
        withCredentials(bindings: [[$class: 'AmazonWebServicesCredentialsBinding', credentialsId: 'aws-role']]) {
          sh './mvnw -Dtest-groups=aws-integration-tests test -B'
        }

      }
    }
  }
}