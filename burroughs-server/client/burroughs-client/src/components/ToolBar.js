import React from 'react';
import { Row } from 'react-bootstrap';
import { FaFolderOpen, FaSave } from 'react-icons/fa';
import { BsFillPlayFill, BsStopFill } from 'react-icons/bs';
import { GiVacuumCleaner } from 'react-icons/gi';
import { connect } from 'react-redux';
import { Spinner } from 'react-bootstrap';
import { saveAs } from 'file-saver';

import {
    executeQuery,
    terminateQuery,
    setKeepTable,
    getStatus
} from '../actions/queryActions';

import { setCode } from '../actions/basicActions';
import { cleanup } from '../actions/topicActions';

class ToolBar extends React.Component {

    constructor(props) {
        super(props);
        this.fileRef = React.createRef();
    }

    componentDidMount() {
        this.props.getStatus();
    }

    getExecButton() {
        let className = "play-button";
        if (this.props.queryActive || (!this.props.tableName || this.props.tableName.length < 1)) {
            className += " disabled";
        }
        else {
            className += " enabled";
        }
        if (this.props.executing) {
            return <Spinner animation="border" size="sm" style={{ marginLeft: '6px' }} />;
        }
        else {
            let clickHandler = () => { };
            if (!this.props.queryActive) {
                clickHandler = e => {
                    this.props.executeQuery(this.props.code.trim());
                };
            }
            return (
                <BsFillPlayFill
                    className={className}
                    style={{ marginLeft: '6px', fontSize: '24px' }}
                    onClick={clickHandler}
                />
            );
        }
    }

    getTerminateButton() {
        let className = "stop-button";
        if (this.props.queryActive) {
            className += " enabled";
        }
        else {
            className += " disabled";
        }

        let clickHandler = () => { };
        if (this.props.queryActive) {
            clickHandler = () => {
                this.props.terminateQuery();
            };
        }


        if (this.props.terminating) {
            return <Spinner animation="border" size="sm" style={{ marginLeft: '6px' }} />;
        }
        else {
            return (
                <BsStopFill
                    className={className} style={{ marginLeft: '6px', fontSize: '23px' }}
                    onClick={clickHandler}
                />
            );
        }
    }

    getCleanupButton() {
        let className = "default-button";
        let clickHandler = () => { };
        if (this.props.queryActive) {
            className += " disabled";
        }
        else {
            clickHandler = () => {
                this.props.cleanup();
            }
        }
        if (this.props.cleaningUp) {
            return <Spinner animation="border" size="sm" style={{ marginLeft: '10px' }} />;
        }
        else {
            return (
                <GiVacuumCleaner
                    style={{ marginLeft: '10px' }} className={className}
                    onClick={clickHandler}
                />
            )
        }
    }

    handleFileUpload(e) {
        if (e.target.files.length > 0) {
            const reader = new FileReader();            
            reader.onload = res => {
                this.props.setCode(res.target.result);
            }
            reader.readAsText(e.target.files[0]);
        }
    }

    render() {
        return (
            <Row style={{ margin: '0px', padding: '3px', paddingLeft: '10px', backgroundColor: '#f0f0f0', width: '100%', height: '5vh', fontSize: '20px' }}>
                <FaFolderOpen 
                    className="default-button" 
                    onClick={() => this.fileRef.current.click()}
                />
                <FaSave 
                    className="default-button" 
                    style={{ marginLeft: '6px' }} 
                    onClick={() => {
                        let blob = new Blob([this.props.code], {type: "text/plain;charset=utf-8"});
                        saveAs(blob, "burroughs_query.sql");                        
                    }}
                />
                {this.getExecButton()}
                {this.getTerminateButton()}
                <div style={{height: '100%', lineHeight: '100%'}}>
                    <input
                        id="keep-table"
                        type="checkbox"
                        style={{ marginLeft: '8px', marginRight: '2px' }}
                        checked={this.props.keepTable}
                        onChange={() => this.props.setKeepTable()}
                    />
                    <label htmlFor="keep-table" style={{ fontSize: '13px' }}>Keep Table</label>
                </div>                
                {this.getCleanupButton()}
                <input 
                    type="file"
                    style={{display: 'none'}}
                    ref={this.fileRef}
                    onChange={e => this.handleFileUpload(e)}
                />
            </Row>
        );
    }
}

const mapStateToProps = state => {
    return {
        code: state.core.code,
        executing: state.query.queryExecuting,
        terminating: state.query.queryTerminating,
        queryActive: state.query.queryActive,
        tableName: state.core.dbInfo.table,
        keepTable: state.query.keepTable,
        cleaningUp: state.topic.cleaningUp
    };
}

export default connect(mapStateToProps, {
    executeQuery, terminateQuery, setKeepTable, getStatus, cleanup, setCode
})(ToolBar);