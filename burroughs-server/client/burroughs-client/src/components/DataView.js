import React from 'react';
import { connect } from 'react-redux';
import { Table } from 'react-bootstrap';

import { loadSnapshot } from '../actions/basicActions';

class DataView extends React.Component {    
    render() {
        if (!this.props.data || this.props.data.length < 1) {
            return <div></div>;
        }
        const headers = this.props.data[0];
        return (
            <Table striped bordered hover>
                <thead>
                    <tr>
                        {headers.map(h => (
                            <th key={'header:' + h}>{h}</th>
                        ))}
                    </tr>
                </thead>
                <tbody>
                    {this.props.data.slice(1).map(row => {
                        return (
                            <tr key={row.toString()}>
                                {row.map(item => {
                                    return(
                                        <td key={row.toString() + item}>{item}</td>
                                    );
                                })}
                            </tr>
                        );
                    })}
                </tbody>
            </Table>
        );
    }
}

const mapStateToProps = state => {
    return {
        data: state.core.data
    };
};

export default connect(mapStateToProps, { loadSnapshot })(DataView);
