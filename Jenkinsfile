pipeline {
  agent any
  stages {
    stage('Checkout') {
      steps {
        git(url: 'https://github.com/Kent1/PCAP-to-Athena.git', branch: 'master')
      }
    }
    stage('Maxmind') {
      steps {
        sh './maxmind/download_maxmind_geo_ip_db.sh'
      }
    }
  }
}