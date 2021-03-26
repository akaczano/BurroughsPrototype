import React from 'react';
import { connect } from 'react-redux';
import { ProgressBar } from 'react-bootstrap';

import { getStatus } from '../actions/queryActions';

class StatusDisplay extends React.Component {
    componentDidMount() {
        this.refreshStatus();        
    }
    refreshStatus = () =>  {                
        if (this.props.queryActive && !this.props.statusRunning) {            
            this.props.getStatus();
        }
        setTimeout(this.refreshStatus, 500);
    }
    render() {
        if (this.props.errorMessage) {
            return <p style={{ color: 'red' }}>{this.props.errorMessage}</p>;
        }
		else if (this.props.queryActive && this.props.status && !this.props.status.tableStatus) {
			return <div>Status not available</div>
		}
        else if (this.props.queryActive && this.props.status){                        
            const { tableStatus, connectorStatus } = this.props.status;            
            const totalProgress = this.props.status.tableStatus.totalProgress / 
                this.props.status.tableStatus.totalWork;
            const timeRemaining = (tableStatus.totalRuntime / totalProgress - tableStatus.totalRuntime) / 1000.0;
            const progressLabel = `${Math.floor(totalProgress * 100)}% (${tableStatus.totalProgress}/${tableStatus.totalWork})`
            return (
                <div className="status-group">
                    <p>Active Query: {this.props.status.queryId}</p>
                    <ProgressBar now={Math.floor(totalProgress * 100)} label={progressLabel}/>                    
                    <p>Process Rate: {tableStatus.processRate} messages/s</p>
                    <p>Messages Processed: {tableStatus.totalMessages}</p>                    
                    <p>Total Run Time: {Math.floor(tableStatus.totalRuntime / 1000)} seconds</p> 
                    <p>Estimated Time Remaing: {Math.floor(timeRemaining)} seconds</p>
                    {connectorStatus?.errors?.map(e => 
                     <p style={{color: 'red'}}>Connector error: <br />{e}</p>   
                    )}
                </div>
            );
        }
        else {
            return <div style={{marginLeft: '3px'}}>No active query.</div>;
        }
    }
}

const mapStateToProps = state => {
    return {
        errorMessage: state.query.queryErrorMessage,
        queryActive: state.query.queryActive,
        status: state.query.status,
        running: state.query.statusRunning
    };
};

export default connect(mapStateToProps, { getStatus })(StatusDisplay);
