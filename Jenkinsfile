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
        sh './mnw clean install'
      }
    }
  }
}