import React from 'react';
import { connect } from 'react-redux';
import { ListGroup, Button, Form } from 'react-bootstrap';

import { executeCommand } from '../actions/producerActions';

const buttonStyle = {
    padding: '2px',
    marginRight: '5px',
    paddingLeft: '5px',
    paddingRight: '5px'
};

class ProducerList extends React.Component {
    state = { limits: {} }

    render() {
        return (
            <ListGroup style={{ width: '100%' }}>
                {this.props.producers.map(p => {
                    let startEnabled = p.status.status === 0 || p.status.status === 3
                        || p.status.status === 4;
                    let pauseEnabled = p.status.status === 1;
                    let resumeEnabled = p.status.status === 2;
                    let killEnabled = p.status.status === 1 || p.status.status === 2;

                    let statusString = '';
                    switch (p.status.status) {
                        case 0:
                            statusString = 'Not started';
                            break;
                        case 1:
                            statusString = 'Running';
                            break;
                        case 2:
                            statusString = 'Paused';
                            break;
                        case 3:
                            statusString = 'Stopped';
                            break;
                        default:
                            statusString = 'Error';
                    }
                    if (!(p.name in this.state.limits)) {
                        let newLimits = {...this.state.limits};
                        newLimits[p.name] = -1;
                        this.setState({limits: newLimits});
                    }
                    let errorDisplay = p.status.errorMessage ?
                        <p style={{ color: 'red' }}>{p.status.errorMessage}</p> : null;
                    return (
                        <ListGroup.Item key={p.name} className="producers-list">
                            <strong>{p.name}</strong>
                            <div style={{ float: 'right' }}>
                                <Button
                                    style={buttonStyle}
                                    variant="secondary"
                                    disabled={!startEnabled}
                                    onClick={() => this.props.executeCommand(p.name, 'start', { limit: this.state.limits[p.name] })}
                                >
                                    Start
                                </Button>
                                <Button
                                    style={buttonStyle}
                                    variant="secondary"
                                    disabled={!pauseEnabled}
                                    onClick={() => this.props.executeCommand(p.name, 'pause')}
                                >
                                    Pause
                                </Button>
                                <Button
                                    style={buttonStyle}
                                    variant="secondary"
                                    disabled={!resumeEnabled}
                                    onClick={() => this.props.executeCommand(p.name, 'resume')}
                                >
                                    Resume
                                </Button>
                                <Button
                                    style={buttonStyle}
                                    variant="secondary"
                                    disabled={!killEnabled}
                                    onClick={() => this.props.executeCommand(p.name, 'kill')}
                                >
                                    Kill
                                </Button>
                                <Form>
                                    <Form.Row>
                                        <Form.Group>
                                            <Form.Label>
                                                Delay
                                        </Form.Label>
                                            <Form.Control
                                                type="number"
                                                value={p.delay}
                                                onChange={e => {
                                                    this.props.executeCommand(p.name, 'setdelay', {delay: e.target.value})
                                                }}
                                            />
                                        </Form.Group>
                                        <Form.Group>
                                            <Form.Label>
                                                Limit
                                            </Form.Label>
                                            <Form.Control
                                                type="number"
                                                value={this.state.limits[p.name]}
                                                onChange={e => {
                                                    let newLimits = {...this.state.limits};
                                                    newLimits[p.name] = e.target.value
                                                    this.setState({ limits: newLimits });
                                                }}
                                                disabled={!startEnabled}
                                            />
                                        </Form.Group>
                                    </Form.Row>
                                </Form>
                            </div>
                            <p>Status: {statusString}</p>
                            <p>Records Produced: {p.status.recordsProduced}</p>
                            <p>Records Lost: {p.status.recordsLost}</p>
                            {errorDisplay}
                        </ListGroup.Item>
                    );
                })}
            </ListGroup>
        );
    }
}

const mapStateToProps = state => {
    return {
        producers: state.producer.producers
    };
};

export default connect(mapStateToProps, { executeCommand })(ProducerList);