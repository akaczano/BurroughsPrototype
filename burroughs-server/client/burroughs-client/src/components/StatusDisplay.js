import React from 'react';
import { connect } from 'react-redux';

import { getStatus } from '../actions/basicActions';

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
        else if (this.props.queryActive && this.props.status){            
            const { tableStatus, connectorStatus } = this.props.status;
            const totalProgress = this.props.status.tableStatus.totalProgress / 
                this.props.status.tableStatus.totalWork;
            const timeRemaining = (tableStatus.totalRuntime / totalProgress - tableStatus.totalRuntime) / 1000.0;
            return (
                <div className="status-group">
                    <p>Active Query: {this.props.status.queryId}</p>
                    <p>Process Rate: {tableStatus.processRate} messages/s</p>
                    <p>Messages Processed: {tableStatus.totalMessages}</p>
                    <p>Total Progress: {Math.floor(totalProgress * 100)}% ({tableStatus.totalProgress}/{tableStatus.totalWork})</p>
                    <p>Total Run Time: {Math.floor(tableStatus.totalRuntime / 1000)} seconds</p> 
                    <p>Estimated Time Remaing: {Math.floor(timeRemaining)} seconds</p>
                </div>
            );
        }
        else {
            return <div>No active query.</div>;
        }
    }
}

const mapStateToProps = state => {
    return {
        errorMessage: state.core.queryErrorMessage,
        queryActive: state.core.queryActive,
        status: state.core.status,
        running: state.core.statusRunning
    };
};

export default connect(mapStateToProps, { getStatus })(StatusDisplay);
