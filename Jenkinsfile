pipeline {
  agent any
  stages {
    stage('Checkout') {
      steps {
        git(url: 'https://github.com/Kent1/PCAP-to-Athena.git', branch: 'master')
      }
    }
    stage('Setting up') {
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
        sh './mvnw jacoco:prepare-agent test -B -Psonar'
      }
    }
    stage('Integration tests') {
      post {
        success {
          junit 'target/surefire-reports/**/*.xml'
        }
      }
      steps {
        withCredentials(bindings: [[$class: 'AmazonWebServicesCredentialsBinding', credentialsId: 'pcap-to-athena-aws-role']]) {
          sh './mvnw -Dtest-groups=aws-integration-tests test -B'
        }

      }
    }
    stage('Sonarqube') {
      steps {
        sh './mvnw sonar:sonar -Psonar'
      }
    }
    stage('Publish') {
      parallel {
        stage('Maven Central') {
          steps {
            configFileProvider([configFile(fileId: 'Engineering_DNS_Belgium_OSSRH_maven_settings', variable: 'MAVEN_SETTINGS')]) {
              withCredentials(bindings: [sshUserPrivateKey(credentialsId: 'Engineering_DNS_Belgium_GPG', keyFileVariable: 'GPG_SECRET_KEY', passphraseVariable: 'GPG_PASSPHRASE')]) {
                sh 'mvn -s $MAVEN_SETTINGS deploy -DskipTests=true -B -U -Prelease -Dgpg.passphrase=$GPG_PASSPHRASE'
              }
            }
          }
        }
        stage('Docker') {
          steps {
            withCredentials(bindings: [[$class: 'AmazonWebServicesCredentialsBinding', credentialsId: 'pcap-to-athena-aws-role-ecr']]) {
              sh 'echo docker'
            }
          }
        }
      }
    }
  }
}
