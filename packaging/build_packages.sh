#!/bin/bash

usage() {
    echo "Usage: $0 [-e EPOCH] [-h] [-r] <deb|rpm|targz>" >&2
    echo "    -e  Epoch number (default is current timestamp, ignored for release)" >&2
    echo "    -h  Display this help message" >&2
    echo "    -r  Building a release" >&2
}

ARGS=$(getopt "e:hr" "${@}")
if [ "$?" != "0" ]; then
    usage
    exit 1
fi

RELEASE="0"
eval set -- "${ARGS}"
while true; do
    case "${1}" in
        -e)
            EPOCH="${2}"
            shift 2
        ;;
        -h)
            usage
            exit 0
        ;;
        -r)
            RELEASE="1"
            shift 1
        ;;
        --)
            shift
            break
        ;;
    esac
done

if [ "$2" != "" ]; then
    echo "Unexpected extra arguments" >&2
    usage
    exit 1
fi


set -e

PACKAGING_DIR=$(cd $(dirname "${0}") ; echo "${PWD}")
TOP_DIR=$(cd "${PACKAGING_DIR}/.." ; echo "${PWD}")
STAGE_DIR="${TOP_DIR}/target/packaging/${1}"


mvn_package() {
    mvn clean package -q -B -U -DskipTests=true -Dfdbsql.release=${RELEASE} >/dev/null
}

# $1 - output bin dir
# $2 - output jar dir
build_client_tools() {
    CLIENT_MVN_VERSION=$(cd "${TOP_DIR}" ; mvn -o org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version |grep '^[0-9]')
    CLIENT_VERSION=${CLIENT_MVN_VERSION%-SNAPSHOT}
    CLIENT_JAR_NAME=""

    echo "Building SQL Layer Client Tools ${CLIENT_VERSION} Release ${RELEASE}"
    pushd .
    cd "${TOP_DIR}"
    mvn_package
    cd target/
    for f in $(ls fdb-sql-layer-client-tools*.jar |grep -v -E 'source|test'); do
        CLIENT_JAR_NAME="$f"
    done
    cd ..
    if [ -z "${CLIENT_JAR_NAME}" ]; then
        echo "No client jar found" >&2
        exit 1
    fi
    mkdir -p "${1}" "${2}/client"
    cd bin
    for f in $(ls fdbsql* |grep -v '\.cmd'); do
        install -m 0755 "${f}" "${1}/"
    done
    cd ..
    install -m 0644 target/${CLIENT_JAR_NAME} "${2}/"
    install -m 0644 target/dependency/* "${2}/client/"
    popd
}

case "${1}" in
    "deb")
        umask 0022
        build_client_tools "${STAGE_DIR}/usr/bin" "${STAGE_DIR}/usr/share/foundationdb/sql"

        cd "${STAGE_DIR}"
        mkdir -p -m 0755 "usr/share/doc/fdb-sql-layer-client-tools/"
        install -m 0644 "${PACKAGING_DIR}/deb/copyright" "usr/share/doc/fdb-sql-layer-client-tools/"

        mkdir DEBIAN
        sed -e "s/VERSION/${CLIENT_VERSION}/g" \
            -e "s/RELEASE/${RELEASE}/g" \
            "${PACKAGING_DIR}/deb/fdb-sql-layer-client-tools.control.in" > \
            DEBIAN/control
        
        cd "usr/share/foundationdb/sql"
        ln -s "${CLIENT_JAR_NAME}" fdb-sql-layer-client-tools.jar

        cd "${STAGE_DIR}"
        fakeroot dpkg-deb --build . "${TOP_DIR}/target"
    ;;

    "rpm")
        BUILD_DIR="${STAGE_DIR}/BUILD"
        build_client_tools "${BUILD_DIR}/usr/bin" "${BUILD_DIR}/usr/share/foundationdb/sql"

        # Releases shouldn't have epochs
        if [ -z "${EPOCH}" -a ${RELEASE} -eq 0 ]; then
            EPOCH=$(date +%s)
            echo "Epoch: ${EPOCH}"
        else
            EPOCH="0"
        fi
        
        cd "${STAGE_DIR}"
        mkdir -p {SOURCES,SRPMS,RPMS/noarch}

        mkdir -p "${BUILD_DIR}/usr/share/doc/fdb-sql-layer-client-tools/"
        cp "${TOP_DIR}/LICENSE.txt" "${BUILD_DIR}/usr/share/doc/fdb-sql-layer-client-tools/LICENSE"

        sed -e "s/VERSION/${CLIENT_VERSION}/g" \
            -e "s/RELEASE/${RELEASE}/g" \
            -e "s/EPOCH/${EPOCH}/g" \
            -e "s/CLIENT_JAR_NAME/${CLIENT_JAR_NAME}/g" \
            "${PACKAGING_DIR}/rpm/fdb-sql-layer-client-tools.spec.in" > \
            fdb-sql-layer-client-tools.spec

        rpmbuild --target=noarch -bb --define "_topdir ${STAGE_DIR}" \
            fdb-sql-layer-client-tools.spec

        mv RPMS/noarch/* "${TOP_DIR}/target/"
    ;;

    "targz")
        build_client_tools "${STAGE_DIR}/bin" "${STAGE_DIR}/lib"

        cd "${STAGE_DIR}"
        cp "${TOP_DIR}/LICENSE.txt" "${STAGE_DIR}/"
        cp "${TOP_DIR}/README.md" "${STAGE_DIR}/"

        BINARY_NAME="fdb-sql-layer-client-tools-${CLIENT_VERSION}-${RELEASE}"
        cd ..
        mv targz "${BINARY_NAME}"
        mkdir targz
        mv "${BINARY_NAME}" targz/
        cd targz
        tar czf "${TOP_DIR}/target/${BINARY_NAME}.tar.gz" "${BINARY_NAME}"
    ;;

    *)
        usage
        if [ "${1}" = "" ]; then
            echo "Package type required" >&2
        else
            echo "Unknown package type: ${1}" >&2
        fi
        exit 1
    ;;
esac

exit 0

