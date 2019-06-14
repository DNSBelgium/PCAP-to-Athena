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
        sh 'echo ./mvnw test -B'
      }
    }
    stage('Integration tests') {
      steps {
        withCredentials(bindings: [[$class: 'AmazonWebServicesCredentialsBinding', credentialsId: 'pcap-to-athena-aws-role']]) {
          sh 'echo ./mvnw -Dtest-groups=aws-integration-tests test -B'
        }
      }
      post {
        success {
          //junit 'target/surefire-reports/**/*.xml'
          sh 'echo coucou'
        }
      }
    }
    stage('Publish') {
      steps {
        configFileProvider([configFile(fileId: 'Engineering_DNS_Belgium_OSSRH_maven_settings', variable: 'MAVEN_SETTINGS')]) {
          withCredentials([sshUserPrivateKey(credentialsId: 'Engineering_DNS_Belgium_GPG', keyFileVariable: 'GPG_SECRET_KEY', passphraseVariable: 'GPG_PASSPHRASE')]) {
            //sh 'echo $GPG_SECRET_KEY | base64 --decode | gpg --import'
            sh 'mvn -s $MAVEN_SETTINGS deploy -DskipTests=true -B -U -Prelease -Dgpg.passphrase=$GPG_PASSPHRASE'
          }
        }
      }
    }
  }
}
