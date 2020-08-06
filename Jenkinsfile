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
                    for (def csvw : findFiles(glob: "out/*.csv-metadata.json")) {
                        String baseName = csvw.name.substring(0, csvw.name.lastIndexOf('.csv-metadata.json'))
                        datasets.add([
                                "csv"     : "out/${baseName}.csv",
                                "metadata": "out/${baseName}.csv-metadata.trig",
                                "csvw"    : "out/${csvw.name}",
                                "output"  : "out/${baseName}",
                                "base"    : baseName
                        ])
                    }
                    writeFile file: "graphs.sparql", text: """SELECT ?md ?ds { GRAPH ?md { [] <http://publishmydata.com/pmdcat#graph> ?ds } }"""
                    for (def dataset : datasets) {
                        if (fileExists(dataset.csv + '.gz')) {
                            sh "pigz -dc '${dataset.csv}.gz' > /tmp/${dataset.base}.csv"
                            sh "cp '${dataset.csvw} /tmp/${basename}-metadata.json"
                            sh "csv2rdf -t '/tmp/${dataset.base}.csv' -u '/tmp/${dataset.base}-metadata.json' -m annotated | pigz > '${dataset.output}.ttl.gz'"
                        } else {
                            sh "csv2rdf -t '${dataset.csv}' -u '${dataset.csvw}' -m annotated | pigz > '${dataset.output}.ttl.gz'"
                        }
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
        stage('Upload Cube') {
            steps {
                script {
                    FAILED_STAGE = env.STAGE_NAME
                    def pmd = pmdConfig("pmd")
                    pmd.drafter
                            .listDraftsets(Drafter.Include.OWNED)
                            .findAll { it['display-name'] == env.JOB_NAME }
                            .each {
                                pmd.drafter.deleteDraftset(it.id)
                            }
                    String id = pmd.drafter.createDraftset(env.JOB_NAME).id
                    for (graph in util.jobGraphs(pmd, id).unique()) {
                        echo "Removing own graph ${graph}"
                        pmd.drafter.deleteGraph(id, graph)
                    }
                    def outputFiles = findFiles(glob: "out/*.ttl.gz")
                    if (outputFiles.length == 0) {
                        error(message: "No output RDF files found")
                    } else {
                        for (def observations : outputFiles) {
                            String baseName = observations.name.substring(0, observations.name.lastIndexOf('.ttl.gz'))
                            def graphs = readJSON(text: readFile(file: "out/${baseName}-graphs.json"))
                            String datasetGraph = graphs.results.bindings[0].ds.value
                            String metadataGraph = graphs.results.bindings[0].md.value
                            echo "Adding ${observations.name}"
                            pmd.drafter.addData(
                                    id,
                                    "${WORKSPACE}/out/${observations.name}",
                                    "text/turtle",
                                    "UTF-8",
                                    datasetGraph
                            )
                            writeFile(file: "out/${baseName}-ds-prov.ttl", text: util.jobPROV(datasetGraph))
                            pmd.drafter.addData(
                                    id,
                                    "${WORKSPACE}/out/${baseName}-ds-prov.ttl",
                                    "text/turtle",
                                    "UTF-8",
                                    datasetGraph
                            )
                            echo "Adding metadata."
                            pmd.drafter.addData(
                                    id,
                                    "${WORKSPACE}/out/${baseName}.csv-metadata.trig",
                                    "application/trig",
                                    "UTF-8",
                                    metadataGraph
                            )
                            writeFile(file: "out/${baseName}-md-prov.ttl", text: util.jobPROV(metadataGraph))
                            pmd.drafter.addData(
                                    id,
                                    "${WORKSPACE}/out/${baseName}-md-prov.ttl",
                                    "text/turtle",
                                    "UTF-8",
                                    metadataGraph
                            )
                        }
                    }
                    for (def codelist : findFiles(glob: "out/codelists/*.ttl.gz")) {
                        String baseName = codelist.name.substring(0, codelist.name.lastIndexOf('.ttl.gz'))
                        String codelistGraph = "${pmd.config.base_uri}/graph/${util.slugise(env.JOB_NAME)}/${baseName}"
                        echo "Adding local codelist ${baseName}"
                        pmd.drafter.addData(
                                id,
                                "${WORKSPACE}/out/codelists/${codelist.name}",
                                "text/turtle",
                                "UTF-8",
                                codelistGraph
                        )
                        writeFile(file: "out/codelists/${baseName}-prov.ttl", text: util.jobPROV(codelistGraph))
                        pmd.drafter.addData(
                                id,
                                "${WORKSPACE}/out/codelists/${baseName}-prov.ttl",
                                "text/turtle",
                                "UTF-8",
                                codelistGraph
                        )
                    }
                }
            }
        }
        stage('Test draft dataset') {
            agent {
                docker {
                    image 'gsscogs/gdp-sparql-tests'
                    reuseNode true
                    alwaysPull true
                }
            }
            steps {
                script {
                    FAILED_STAGE = env.STAGE_NAME
                    pmd = pmdConfig("pmd")
                    String draftId = pmd.drafter.findDraftset(env.JOB_NAME, Drafter.Include.OWNED).id
                    String endpoint = pmd.drafter.getDraftsetEndpoint(draftId)
                    String dspath = util.slugise(env.JOB_NAME)
                    String datasetGraph = "${pmd.config.base_uri}/graph/${dspath}"
                    String metadataGraph = "${pmd.config.base_uri}/graph/${dspath}-metadata"
                    String TOKEN = pmd.drafter.getToken()
                    wrap([$class: 'MaskPasswordsBuildWrapper', varPasswordPairs: [[password: TOKEN, var: 'TOKEN']]]) {
                        sh "sparql-test-runner -i -t /usr/local/tests/qb -s '${endpoint}?union-with-live=true&timeout=180' -k '${TOKEN}' -p \"dsgraph=<${datasetGraph}>\" -r 'reports/TESTS-${dspath}-qb.xml'"
                        sh "sparql-test-runner -i -t /usr/local/tests/pmd/pmd4 -s '${endpoint}?union-with-live=true&timeout=180' -k '${TOKEN}' -p \"dsgraph=<${datasetGraph}>,mdgraph=<${metadataGraph}>\" -r 'reports/TESTS-${dspath}-pmd.xml'"
                    }
                }
            }
        }
        stage('Draftset') {
            parallel {
                stage('Submit for review') {
                    when {
                        expression {
                            def info = readJSON(text: readFile(file: "info.json"))
                            if (info.containsKey('load') && info['load'].containsKey('publish')) {
                                return !info['load']['publish']
                            } else {
                                return true
                            }
                        }
                    }
                    steps {
                        script {
                            FAILED_STAGE = env.STAGE_NAME
                            pmd = pmdConfig("pmd")
                            String draftId = pmd.drafter.findDraftset(env.JOB_NAME, Drafter.Include.OWNED).id
                            pmd.drafter.submitDraftsetTo(draftId, Drafter.Role.EDITOR, null)
                        }
                    }
                }
                stage('Publish') {
                    when {
                        expression {
                            def info = readJSON(text: readFile(file: "info.json"))
                            if (info.containsKey('load') && info['load'].containsKey('publish')) {
                                return info['load']['publish']
                            } else {
                                return false
                            }
                        }
                    }
                    steps {
                        script {
                            FAILED_STAGE = env.STAGE_NAME
                            pmd = pmdConfig("pmd")
                            String draftId = pmd.drafter.findDraftset(env.JOB_NAME, Drafter.Include.OWNED).id
                            pmd.drafter.publishDraftset(draftId)
                        }
                    }
                }
            }
        }
    }
    post {
        always {
            archiveArtifacts 'out/'
            junit allowEmptyResults: true, testResults: 'reports/**/*.xml'
        }
    }
}