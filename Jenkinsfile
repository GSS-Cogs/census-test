@Library('pmd@family-pmd4') _

import uk.org.floop.jenkins_pmd.Drafter

def FAILED_STAGE

pipeline {
    agent {
        label 'master'
    }
    environment {
        JOB_ID = util.getJobID()
    }
    stages {
        stage('Clean') {
            steps {
                script {
                    FAILED_STAGE = env.STAGE_NAME
                    sh "rm -rf out"
                }
            }
        }
        stage('Tidy CSV') {
            agent {
                docker {
                    image 'gsscogs/databaker'
                    reuseNode true
                    alwaysPull true
                }
            }
            steps {
                script {
                    FAILED_STAGE = env.STAGE_NAME
                    ansiColor('xterm') {
                        if (fileExists("main.py")) {
                            sh "jupytext --to notebook *.py"
                        }
                        sh "jupyter-nbconvert --output-dir=out --ExecutePreprocessor.timeout=None --execute 'main.ipynb'"
                    }
                }
            }
        }
        stage('Data Cube') {
            agent {
                docker {
                    image 'gsscogs/csv2rdf'
                    reuseNode true
                    alwaysPull true
                }
            }
            steps {
                script {
                    FAILED_STAGE = env.STAGE_NAME
                    def datasets = []
                    for (def csv : findFiles(glob: "out/*.csv")) {
                        if (fileExists("out/${csv.name}-metadata.json")) {
                            String baseName = csv.name.take(csv.name.lastIndexOf('.'))
                            datasets.add([
                                    "csv"     : "out/${csv.name}",
                                    "metadata": "out/${csv.name}-metadata.trig",
                                    "csvw"    : "out/${csv.name}-metadata.json",
                                    "output"  : "out/${baseName}"
                            ])
                        }
                    }
                    writeFile file: "graphs.sparql", text: """SELECT ?md ?ds { GRAPH ?md { [] <http://publishmydata.com/pmdcat#graph> ?ds } }"""
                    for (def dataset : datasets) {
                        sh "csv2rdf -t '${dataset.csv}' -u '${dataset.csvw}' -m annotated | pigz > '${dataset.output}.ttl.gz'"
                        sh "sparql --data='${dataset.metadata}' --query=graphs.sparql --results=JSON > '${dataset.output}-graphs.json'"
                    }
                }
            }
        }
        stage('Local Codelists') {
            agent {
                docker {
                    image 'gsscogs/csv2rdf'
                    reuseNode true
                    alwaysPull true
                }
            }
            steps {
                script {
                    FAILED_STAGE = env.STAGE_NAME
                    def codelists = []
                    for (def metadata : findFiles(glob: "codelists/*.csv-metadata.json") +
                            findFiles(glob: "out/codelists/*.csv-metadata.json")) {
                        String baseName = metadata.name.substring(0, metadata.name.lastIndexOf('.csv-metadata.json'))
                        String dirName = metadata.path.take(metadata.path.lastIndexOf('/'))
                        codelists.add([
                                "csv"   : "${dirName}/${baseName}.csv",
                                "csvw"  : "${dirName}/${baseName}.csv-metadata.json",
                                "output": "out/codelists/${baseName}"
                        ])
                    }
                    sh "mkdir -p out/codelists"
                    for (def codelist : codelists) {
                        sh "csv2rdf -t '${codelist.csv}' -u '${codelist.csvw}' -m annotated | pigz > '${codelist.output}.ttl.gz'"
                    }
                }
            }
        }
    }
    post {
        always {
            archiveArtifacts '*.ttl.gz'
        }
    }
}