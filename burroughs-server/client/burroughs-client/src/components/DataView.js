import React from 'react';
import { connect } from 'react-redux';
import { Form, Table, Spinner, Button } from 'react-bootstrap';
import { BsFillPlayFill } from 'react-icons/bs';

import { loadSnapshot } from '../actions/basicActions';

class DataView extends React.Component {
    state = { queryText: '' }
    getBody() {
        if (this.props.dataLoading) {
            return (
                <Spinner animation="border" />
            );
        }
        else if (this.props.dataError) {
            return (
                <p style={{ color: 'red' }}>{this.props.dataError}</p>
            );
        }
        else if (!this.props.data || this.props.data.length < 1) {
            return <div>Data not available</div>;
        }
        else {
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
                                        return (
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

    render() {
        return (
            <div>
                <Form onSubmit={e => {
                    e.preventDefault();
                    this.props.loadSnapshot(this.state.queryText);
                }}>
                    <Form.Row style={{marginBottom: '5px', marginLeft: '2px'}}>
                        <Form.Control
                            type="text"
                            value={this.state.queryText}
                            onChange={e => this.setState({ queryText: e.target.value })}
                            style={{ width: '60vw' }}
                        />
                        <Button 
                            onClick={() => this.props.loadSnapshot(this.state.queryText)}
                            variant="secondary"
                            style={{marginLeft: '18px'}}
                            disabled={this.props.dataLoading}
                        >
                            <BsFillPlayFill 
                                style={{color: '#35e871', fontSize: '24px'}}                                
                            />
                        </Button>
                    </Form.Row>
                </Form>
                {this.getBody()}
            </div>
        );
    }
}

const mapStateToProps = state => {
    return {
        data: state.core.data,
        dataLoading: state.core.dataLoading,
        dataError: state.core.dataError
    };
};

export default connect(mapStateToProps, { loadSnapshot })(DataView);
